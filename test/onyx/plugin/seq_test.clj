(ns onyx.plugin.seq-test
  (:require [aero.core :refer [read-config]]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is]]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.tasks
             [seq :as seq]
             [core-async :as ca]]
            [onyx.plugin
             [seq]
             [core-async :refer [take-segments! get-core-async-channels]]]))

(defn build-job [filename batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job {:workflow [[:in :out]]
                  :catalog []
                  :lifecycles []
                  :windows []
                  :triggers []
                  :flow-conditions []
                  :task-scheduler :onyx.task-scheduler/balanced}]
    (-> base-job
        (add-task (seq/buffered-file-reader :in "test-resources/lines.txt" batch-settings))
        (add-task (ca/output :out batch-settings)))))

(deftest seq-test
  (let [{:keys [env-config peer-config]} (read-config
                                          (io/resource "config.edn")
                                          {:profile :test})
        job (build-job "test-resources/lines.txt" 10 1000)
        {:keys [out]} (get-core-async-channels job)]
    (with-test-env [test-env [2 env-config peer-config]]
      (onyx.test-helper/validate-enough-peers! test-env job)
      (onyx.api/submit-job peer-config job)
      (is (= (set (butlast (take-segments! out)))
             #{{:val "line1"} {:val "line2"} {:val "line3"}
               {:val "line4"} {:val "line5"} {:val "line6"}
               {:val "line7"} {:val "line8"} {:val "line9"}})))))
