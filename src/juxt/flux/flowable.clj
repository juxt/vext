;; Copyright © 2020, JUXT LTD.

(ns juxt.flux.flowable
  (:refer-clojure :exclude [map count merge-with repeat])
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
