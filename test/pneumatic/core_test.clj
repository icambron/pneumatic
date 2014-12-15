(ns pneumatic.core-test
  (:require [midje.sweet :refer :all]
            [pneumatic.core :refer :all]
            [clojure.core.async :as async :refer [go-loop go]]))

;; poor example; use a transducer instead
(defn- almost-adder [n]
  (fn [in]
    (let [out (async/chan)]
      (async/go-loop []
        (let [v (async/<! in)]
          (if (nil? v)
              (async/close! out)
              (do
                (async/>! out (+ n v))
                (recur)))))
      out)))

(defn- adder [n]
  (guarantee-size 1
    (fn [[in]]
      [((almost-adder n) in)])))

(defn- numbers [] [(async/to-chan (take 10 (iterate inc 0)))])
(defn- more-numbers [] [(async/to-chan (take 10 (iterate inc 12)))])

(defn- sequentialize [channel]
  (let [value (async/<!! channel)]
    (if (nil? value) nil (cons value (lazy-seq (sequentialize channel))))))

(defn- sequentialize-nth [channels n]
  (-> channels (nth n) sequentialize))

(defn- sequentialize-first [channels]
  (sequential-nth channels 0))

(defn- sequentialize-all [channels]
  (map sequentialize channels))

(fact "start"
  (fact "combines a chain of stages with a source function"
    (sequentialize-first (start numbers (adder 1) (adder 2))) => (take 10 (iterate inc 3)))
  (fact "combines a chaine of stages with a source vector"
    (sequentialize-first (start (numbers) (adder 1) (adder 2))) => (take 10 (iterate inc 3))))

(fact "chain"
  (fact "combines two stages serially"
    (sequentialize-first ((chain (adder 1) (adder 2)) (numbers))) => (take 10 (iterate inc 3))))

(fact "slim"
  (fact "wraps a ch -> ch into a stage"
    (sequentialize-first (start numbers (slim (almost-adder 3)))) => (take 10 (iterate inc 3))))

(fact "pure"
  (fact "maps a pure function"
    (sequentialize-first (start numbers (pure (partial + 3)))) => (take 10 (iterate inc 3)))
  (fact "maps a pure function in paraellel"
    (sequentialize-first (start numbers (pure (partial + 3) 5))) => (take 10 (iterate inc 3)))
  (fact "works on multiple channels"
    (sequentialize-all (start (concat (numbers) (more-numbers)) (pure (partial + 3)))) => [(take 10 (iterate inc 3)) (take 10 (iterate inc 15))]))

(fact "xduce"
  (fact "applies the transducer"
    (sequentialize-first (start numbers (xduce (filter even?)))) => (take 5 (iterate #(+ 2 %) 0)))
  (fact "applies the transducer in parallel"
    (sequentialize-first (start numbers (xduce (filter even?) 5))) => (take 5 (iterate #(+ 2 %) 0)))
  (fact "works on multiple channels"
    (sequentialize-all (start (concat (numbers) (more-numbers)) (xduce (filter even?)))) => [(take 5 (iterate #(+ 2 %) 0)) (take 5 (iterate #(+ 2 %) 12))]))

(fact "bury-all"
  (fact "ignores the pipeline"
    (start numbers (bury-all)) => []))

(fact "terminate"
  (fact "executes the function but doesn't provide any channels"
    (let [output (transient [])
          terminator (fn [[ch]]
                       (loop []
                         (let [i (async/<!! ch)]
                           (cond (not (nil? i))
                                 (do
                                   (conj! output i)
                                   (recur))))))]
        (start numbers (terminate terminator))
        (persistent! output))
    =>
    (take 10 (iterate inc 0))))

(fact "inject"
  (fact "injects channels at the end by default"
    (let [pipeline (start
                      (concat (numbers) (numbers))
                      (xduce (map (partial * 2)))
                      (inject (numbers)))]
      (sequentialize-nth pipeline 2) => (take 10 (iterate inc 0))))

  (fact "injects channels in locations specified by the second argument"
    (let [pipeline (start
                      (concat (numbers) (numbers))
                      (xduce (map (partial + 2)))
                      (inject (numbers) (restructure [[:a :b] [:c]] [:a :c :b])))]
      (count pipeline) => 3
      (sequentialize-nth pipeline 0) => (take 10 (iterate inc 2))
      (sequentialize-nth pipeline 1) => (take 10 (iterate inc 0))
      (sequentialize-nth pipeline 2) => (take 10 (iterate inc 2)))))

