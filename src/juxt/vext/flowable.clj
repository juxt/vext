;; Copyright © 2020, JUXT LTD.

;; Clojure shim upon io.reactivex.Flowable

(ns juxt.vext.flowable
  (:refer-clojure :exclude [map count merge-with repeat reduce])
  (:import
   (io.reactivex.functions Function)))

(defn do-on-complete [f flowable]
  (.doOnComplete
   flowable
   (reify io.reactivex.functions.Action
     (run [_]
       (f)))))

(defn do-on-terminate [f flowable]
  (.doOnTerminate
   flowable
   (reify io.reactivex.functions.Action
     (run [_]
       (f)))))

(defn map [f flowable]
  (.flatMap
   flowable
   (reify Function
     (apply [_ item]
       (f item)))))

(defn subscribe
  ([flowable]
   (.subscribe flowable))
  ([subscriber flowable]
   (.subscribe flowable subscriber)))

(defn ignore-elements [flowable]
  (.ignoreElements flowable))

(defn count [flowable]
  (.count flowable))

(defn publish [flowable]
  (.publish flowable))

(defn merge-with [other flowable]
  (.mergeWith flowable other))

(defn repeat [n flowable]
  (.repeat flowable n))

(defn as-consumer [f]
  (reify io.reactivex.functions.Consumer
    (accept [_ t] (f t))))

(defn reduce [seed bifn f]
  (.reduce
   f
   seed
   (reify io.reactivex.functions.BiFunction
     (apply [_ acc i] (bifn acc i)))))
