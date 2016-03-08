(ns onyx.plugin.seq-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [taoensso.timbre :refer [info]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.plugin.seq]
            [onyx.api])
  (:import [java.io BufferedReader FileReader]))

(def id (java.util.UUID/randomUUID))

(def env-config 
  {:onyx/tenancy-id id
   :zookeeper/address "127.0.0.1:2188"
   :zookeeper/server? true
   :zookeeper.server/port 2188})

(def peer-config 
  {:onyx/tenancy-id id
   :zookeeper/address "127.0.0.1:2188"
   :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
   :onyx.messaging.aeron/embedded-driver? true
   :onyx.messaging/allow-short-circuit? false
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port 40200
   :onyx.messaging/bind-addr "localhost"})

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def n-messages 100)

(def batch-size 20)

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.seq/input
    :onyx/type :input
    :onyx/medium :seq
    :seq/checkpoint? true
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Documentation for your datasource"}

   {:onyx/name :out
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}])

(def workflow [[:in :out]])

(def out-chan (chan (sliding-buffer (inc n-messages))))

(defn inject-in-reader [event lifecycle]
  (let [rdr (FileReader. (:buffered-reader/filename lifecycle))] 
    {:seq/rdr rdr
     :seq/seq (map (partial hash-map :val) (line-seq (BufferedReader. rdr)))}))

(defn close-reader [event lifecycle]
  (.close (:seq/rdr event)))

(defn inject-out-ch [event lifecycle]
  {:core.async/chan out-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-reader
   :lifecycle/after-task-stop close-reader})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(def lifecycles
  [{:lifecycle/task :in
    :buffered-reader/filename "resources/lines.txt"
    :lifecycle/calls ::in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.seq/reader-calls}
   {:lifecycle/task :out
    :lifecycle/calls ::out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(def v-peers (onyx.api/start-peers 2 peer-group))

(onyx.api/submit-job
 peer-config
 {:catalog catalog
  :workflow workflow
  :lifecycles lifecycles
  :task-scheduler :onyx.task-scheduler/balanced})

(def results (take-segments! out-chan))

(deftest testing-output
  (testing "Input is received at output"
    (let [expected #{{:val "line1"} {:val "line2"} {:val "line3"} {:val "line4"} {:val "line5"} {:val "line6"} {:val "line7"} {:val "line8"} {:val "line9"}}]
    (is (= expected (set (butlast results))))
    (is (= :done (last results))))))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)
