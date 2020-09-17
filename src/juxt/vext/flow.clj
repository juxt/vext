;; Copyright © 2020, JUXT LTD.

(ns juxt.vext.flow
  (:require [juxt.flow.protocols :as flow]))

;; Adapt org.reactivestreams.Subscription to the Clojure protocol
;; See https://www.reactive-streams.org/reactive-streams-1.0.2-javadoc/org/reactivestreams/Subscription.html
(extend-protocol flow/Subscription
  org.reactivestreams.Subscription
  (cancel [s] (.cancel s))
  (request [s n] (.request s n)))

;; Adapt Vert.x subscriber to the Clojure protocol (e.g. the Vert.x HTTP response)
(extend-protocol flow/Subscriber
  io.vertx.reactivex.WriteStreamSubscriber
  (on-complete [s]
    (.onComplete s))
  (on-error [s t]
    (.onError s t))
  (on-next [s item]
    (.onNext s item))
  (on-subscribe [s subscription]
    (.onSubscribe
     s
     (reify org.reactivestreams.Subscription
       (cancel [_] (flow/cancel subscription))
       (request [_ n] (flow/request subscription n))))))

(extend-protocol flow/Publisher
  org.reactivestreams.Publisher
  (subscribe [publisher subscriber]
    (.subscribe
     publisher
     (reify
       org.reactivestreams.Subscriber
       (onNext [_ item] (flow/on-next subscriber item))
       (onSubscribe [_ subscription] (flow/on-subscribe subscriber subscription))
       (onError [_ t] (flow/on-error subscriber t))
       (onComplete [_] (flow/on-complete subscriber))))))
