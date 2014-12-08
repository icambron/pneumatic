(ns pneumatic.example
  (:use [pneumatic.core])
  (:require [clojure.core.async :as async]))

;generates 0 through 9 and the closes the channel
(defn numbers []
  (let [out (async/chan)]
    (async/onto-chan out (take 10 (iterate inc 0)))
    (vector out)))

; assumes one channel, grabs things from it, adds n, and pushes them onto its one output channel
(defn adder [n]
  (guarantee-size 1
    (fn [[in]]
      (let [out (async/chan)]
      (async/go-loop []
        (let [v (async/<! in)]
          (if (nil? v)
              (async/close! out)
              (do
                (async/>! out (+ n v))
                (recur)))))
      (vector out)))))

(defn doubler [in]
  (let [out (async/chan)]
    (async/go-loop []
      (let [v (async/<! in)]
        (if (nil? v)
            (async/close! out)
            (do
              (async/>! out (* 2 v))
              (recur)))))
    out))

; print everything that comes to it
(defn printer [ins]
  (async/go-loop []
    (let [[v p] (async/alts! ins)]
      (if (not (nil? v))
        (do
          (println v)
          (recur))))))

;run that as a pipeline
(defn simple-chain []
  (start
     numbers
     (adder 4)
     (slim doubler)
     (terminate printer))) ; prints 4 through 13

;a stage with one input and two outputs
(defn splitter [[in]]
  (let [left (async/chan)
        right (async/chan)]
       (async/go-loop []
         (let [v (async/<! in)]
              (if (nil? v)
                  (do
                    (async/close! left)
                    (async/close! right))
                  (do
                    (async/>! left (+ v 5))
                    (async/>! right (- v 8))
                    (recur)))))
       (vector left right)))

; run as a pipeline
(defn split-chain []
  (start
    numbers
    (adder 4)
    splitter
    (divvy-all
      (slim doubler)
      (adder -1))
    (terminate printer)))

;lots more!
