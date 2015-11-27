## onyx-seq

Onyx plugin for reading from a seq. The seq will be read from in way that is
approriate for use with lazy-seqs. Therefore, this plugin can be useful for use
with datomic.api/datoms calls, slow lazy calculations, line-seq / buffered reading, etc.

#### Build Status

[![Circle CI](https://circleci.com/gh/onyx-platform/onyx-seq.svg?style=svg)](https://circleci.com/gh/onyx-platform/onyx-seq)

#### Installation

In your project file:

```clojure
[org.onyxplatform/onyx-seq "0.8.2.2-SNAPSHOT"]
```

```clojure
(:require [onyx.plugin.seq])
```

#### Functions

##### sample-entry

Catalog entry:

```clojure
{:onyx/name :entry-name
 :onyx/plugin :onyx.plugin.seq/input
 :onyx/type :input
 :onyx/medium :seq
 :seq/elements-per-segment 2
 :seq/checkpoint? true
 :onyx/batch-size batch-size
 :onyx/max-peers 1
 :onyx/doc "Reads segments from seq"}
```

Lifecycle entry:

```clojure
[{:lifecycle/task :in
  :lifecycle/calls :onyx.plugin.seq/reader-calls}]
```

##### Checkpointing

onyx-seq will checkpoint the state of the read results. In the case of a
virtual peer crash, the new virtual peer will drop from the seq until it has
reached the first non fully acked segment. In order for this process to work,
the seq that is read from must be reproducible on restart. If it is not, please
disable checkpointing via :seq/checkpoint?.

##### Example Use - Buffered Line Reader

```clojure
(defn inject-in-reader [event lifecycle]
  (let [rdr (FileReader. (:buffered-reader/filename lifecycle))] 
    {:seq/rdr rdr
     :seq/seq (line-seq (BufferedReader. rdr))}))

(defn close-reader [event lifecycle]
  (.close (:seq/rdr event)))

(def in-calls
  {:lifecycle/before-task-start inject-in-reader
   :lifecycle/after-task-stop close-reader})

;; lifecycles

(def lifecycles
  [{:lifecycle/task :in
    :buffered-reader/filename "resources/lines.txt"
    :lifecycle/calls ::in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.seq/reader-calls}])
```

#### Attributes

| key                          | type      | description
|------------------------------|-----------|------------
|`:seq/elements-per-segment`   | `integer` | The number of elements to compress into a single segment

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2015 Distributed Masonry LLC

Distributed under the Eclipse Public License, the same as Clojure.
