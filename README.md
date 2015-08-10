## onyx-lazy-seq

Onyx plugin for lazy-seq.

#### Installation

In your project file:

```clojure
[onyx-lazy-seq "0.6.0"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.lazy-seq])
```

#### Functions

##### sample-entry

Catalog entry:

```clojure
{:onyx/name :entry-name
 :onyx/ident :lazy-seq/task
 :onyx/type :input
 :onyx/medium :lazy-seq
 :onyx/batch-size batch-size
 :onyx/doc "Reads segments from lazy-seq"}
```

Lifecycle entry:

```clojure
[{:lifecycle/task :your-task-name
  :lifecycle/calls :onyx.plugin.lazy-seq/lifecycle-calls}]
```

#### Attributes

|key                           | type      | description
|------------------------------|-----------|------------
|`:lazy-seq/attr`            | `string`  | Description here.

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2015 FIX ME

Distributed under the Eclipse Public License, the same as Clojure.
