(ns onyx.tasks.seq
  (:require [schema.core :as s]
            [onyx.schema :as os])
  (:import [java.io BufferedReader FileReader]))

(def BufferedFileReaderTaskMap
  {(s/optional-key :seq/checkpoint?) s/Bool
   :buffered-file-reader/filename s/Str
   (os/restricted-ns :seq) s/Any})

(defn inject-in-reader [event lifecycle]
  (let [rdr (FileReader. (:buffered-file-reader/filename (:onyx.core/task-map event)))]
    {:seq/rdr rdr
     :seq/seq (map (partial hash-map :val) (line-seq (BufferedReader. rdr)))}))

(defn close-reader [event lifecycle]
  (.close (:seq/rdr event)))

(def buffered-file-reader-lifecycles
  {:lifecycle/before-task-start inject-in-reader
   :lifecycle/after-task-stop close-reader})

(s/defn ^:always-validate buffered-file-reader
  ([task-name :- s/Keyword opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.seq/input
                             :onyx/type :input
                             :onyx/medium :seq
                             :onyx/max-peers 1
                             :seq/checkpoint? true}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls ::buffered-file-reader-lifecycles}
                        {:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.seq/reader-calls}]}
    :schema {:task-map BufferedFileReaderTaskMap}})
  ([task-name :- s/Keyword
    filename :- s/Str
    task-opts :- {s/Any s/Any}]
   (buffered-file-reader task-name (merge {:buffered-file-reader/filename filename}
                                          task-opts))))
