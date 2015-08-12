(ns onyx.plugin.seq
  (:require [onyx.peer.function :as function]
            [onyx.peer.pipeline-extensions :as p-ext]
            [clojure.core.async :refer [chan >! >!! <!! close! go thread timeout alts!! 
                                        go-loop sliding-buffer]]
            [onyx.static.default-vals :refer [defaults arg-or-default]]
            [onyx.extensions :as extensions]
            [onyx.types :as t]
            [taoensso.timbre :refer [debug info fatal] :as timbre]))

(def seq-defaults
  {:seq/commit-ch-size 1000})

(defn input-drained? [pending-messages batch]
  (and (= 1 (count @pending-messages))
       (= (count batch) 1)
       (= (:message (first batch)) :done)))

(defn safe-elements-per-segment [task-map]
  (or (:seq/elements-per-segment task-map)
      (throw (ex-info ":seq/elements-per-segment missing from task-map." task-map))))

(defn close-read-seq-resources 
  [{:keys [seq/producer-ch seq/commit-ch seq/read-ch] :as event} lifecycle]
  (close! read-ch)
  (close! commit-ch)
  (close! producer-ch))

(defn start-commit-loop! [commit-ch log task-id]
  (go-loop []
           (when-let [content (<!! commit-ch)] 
             (extensions/force-write-chunk log :chunk content task-id)
             (recur))))

(defn inject-read-seq-resources
  [{:keys [onyx.core/task-map onyx.core/log onyx.core/task-id onyx.core/pipeline] :as event} lifecycle]
  (when-not (= 1 (:onyx/max-peers task-map))
    (throw (ex-info "read seq tasks must set :onyx/max-peers 1" task-map)))
  (let [_ (extensions/write-chunk log :chunk {:chunk-index -1 :status :incomplete} task-id)
        content (extensions/read-chunk log :chunk task-id)
        elements-per-segment (safe-elements-per-segment task-map)]
    (if (= :complete (:status content))
      (throw (Exception. "Restarted task and it was already complete. This is currently unhandled."))
      (let [ch (:read-ch pipeline)
            start-index (:chunk-index content)
            num-ignored (* start-index elements-per-segment)
            commit-loop-ch (when (false? (:seq/checkpoint? task-map)) 
                             (start-commit-loop! (:commit-ch pipeline) log task-id))
            producer-ch (thread
                          (try
                            (loop [chunk-index (inc start-index)
                                   seq-seq (seq (drop num-ignored (:seq/seq event)))]
                              (when seq-seq 
                                (if (>!! ch (assoc (t/input (java.util.UUID/randomUUID)
                                                            {:elements (take elements-per-segment seq-seq)})
                                                   :chunk-index chunk-index))
                                  (recur (inc chunk-index) 
                                         (seq (drop elements-per-segment seq-seq))))))
                            (>!! ch (t/input (java.util.UUID/randomUUID) :done))
                            (catch Exception e
                              (fatal e))))]
        
        {:seq/read-ch ch
         :seq/commit-ch (:commit-ch pipeline)
         :seq/producer-ch producer-ch
         :seq/drained? (:drained pipeline)
         :seq/top-chunk-index (:top-chunk-index pipeline)
         :seq/top-acked-chunk-index (:top-acked-chunk-index pipeline)
         :seq/pending-chunk-indices (:pending-chunk-indices pipeline) 
         :seq/pending-messages (:pending-messages pipeline)}))))

(def reader-calls 
  {:lifecycle/before-task-start inject-read-seq-resources
   :lifecycle/after-task-stop close-read-seq-resources})

(defn highest-acked-chunk [starting-index max-index pending-chunk-indices]
  (loop [max-pending starting-index]
    (if (or (pending-chunk-indices (inc max-pending))
            (= max-index max-pending))
      max-pending
      (recur (inc max-pending)))))

(defrecord SeqInput [log task-id max-pending batch-size batch-timeout pending-messages drained? 
                     top-chunk-index top-acked-chunk-index pending-chunk-indices read-ch commit-ch]
  p-ext/Pipeline
  (write-batch 
    [this event]
    (function/write-batch event))

  (read-batch 
    [_ event]
    (let [pending (count (keys @pending-messages))
          max-segments (min (- max-pending pending) batch-size)
          timeout-ch (timeout batch-timeout)
          batch (->> (range max-segments)
                     (keep (fn [_] (first (alts!! [read-ch timeout-ch] :priority true)))))]
      (doseq [m batch]
        (when-let [chunk-index (:chunk-index m)] 
          (swap! top-chunk-index max chunk-index)
          (swap! pending-chunk-indices conj chunk-index))
        (swap! pending-messages assoc (:id m) m))
      (when (and (= 1 (count @pending-messages))
                 (= (count batch) 1)
                 (= (:message (first batch)) :done))
        (>!! commit-ch {:status :complete})
        (reset! drained? true))
      {:onyx.core/batch batch}))

  (seal-resource [this event])

  p-ext/PipelineInput
  (ack-segment [_ _ segment-id]
    (let [chunk-index (:chunk-index (@pending-messages segment-id))]
      (swap! pending-chunk-indices disj chunk-index)
      (let [new-top-acked (highest-acked-chunk @top-acked-chunk-index @top-chunk-index @pending-chunk-indices)]
        (>!! commit-ch {:chunk-index new-top-acked :status :incomplete})
        (reset! top-acked-chunk-index new-top-acked))
      (swap! pending-messages dissoc segment-id)))

  (retry-segment 
    [_ event segment-id]
    (when-let [msg (get @pending-messages segment-id)]
      (swap! pending-messages dissoc segment-id) 
      (>!! read-ch (assoc msg :id (java.util.UUID/randomUUID)))))

  (pending?
    [_ _ segment-id]
    (get @pending-messages segment-id))

  (drained? 
    [_ _]
    @drained?))

(defn input [{:keys [onyx.core/log onyx.core/task-id 
                     onyx.core/task-map] :as event}]
  (let [max-pending (arg-or-default :onyx/max-pending task-map)
        batch-timeout (arg-or-default :onyx/batch-timeout task-map)
        batch-size (:onyx/batch-size task-map)
        pending-messages (atom {})
        drained? (atom false)
        top-chunk-index (atom -1)
        top-acked-chunk-index (atom -1)
        pending-chunk-indices (atom #{})
        read-ch (chan (or (:seq/read-buffer task-map) 1000))
        commit-ch (chan (sliding-buffer 1))] 
    (->SeqInput log task-id max-pending batch-size batch-timeout pending-messages drained? 
                top-chunk-index top-acked-chunk-index pending-chunk-indices
                read-ch commit-ch)))
