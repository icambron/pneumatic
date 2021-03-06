(ns pneumatic.core
  (:require [clojure.walk :as walk]
            [clojure.core.async :as async]))

(defn- new-channels
  ([xfrom] (new-channels xfrom #(async/chan)))
  ([xfrom channel-creator] (map (fn [_] (channel-creator)) xfrom)))

;; utilities

(defn guarantee-size
  "Wrap a stage with a assertion that it's provided exactly n input channels"
  [n stage]
  (fn [xfrom]
    (assert (= n (count xfrom)) (str "You may only pass " n  " channel(s) to this stage."))
    (stage xfrom)))

(defn restructure
  "Blunt instrument for reorganizing sequences."
  [from to]
  (fn [& xs]
    (let [mapping (zipmap (flatten from) (flatten xs))]
      (walk/prewalk-replace mapping to))))

(defn full-spread [xs] (map vector xs))

;; stages

(defn chain [& stages]
  "Combine multiple stages serially"
  (fn [xfrom]
    (reduce #(%2 %1) xfrom stages)))

(defn xduce
  "A stage consisting of an optionally parallelized transducer"
  ([xf] (xduce xf 1))
  ([xf n] (fn [xfrom]
             (let [xto (new-channels xfrom)]
                  (doseq [[from to] (map list xfrom xto)]
                         (async/pipeline n to xf from))
                  xto))))

(defn slim
  "Convenience that creates a stage out of a function that takes an input channel and returns an output channel. Really just (ch -> ch) -> ([ch] -> [ch])"
  [func]
  (guarantee-size 1 #(vector (func (first %)))))

(defn pure
  "Convenience for using a pure function as a stage. You should really just using a mapping transducer, which is all this does for you."
  ([func] (pure func 1))
  ([func n] (xduce (map func) n)))

(defn bury-all
  "Drop all channels, useful when combined with shunt"
  []
  (fn [xfrom] []))

(defn terminate [func]
  "Convenience that creates a stage out of a function that takes a vector of input channels and returns nothing relevant."
  (fn [xfrom]
    (func xfrom)
    []))

(defn divvy-by
  "Split up the input stages into different buckets and supply each bucket to different stages. The bucketization is controlled by `which`, a function like [ch] -> [[ch]]. The vector returned by `which` must be at least the length as `stages`. If there are fewer stages than buckets, additional channels are flattened and passed on untouched."
  [which & stages]
  (fn [xfrom]
    (let [restructured      (which xfrom)
          re-count          (count restructured)
          stage-count       (count stages)
          [staged leftover] (split-at stage-count xfrom)]

         (assert (<= re-count stage-count) "You must split the channels into at at least the number of buckets as you provide stages to handle them.")
         (->>
           (map #(if (sequential? %) (flatten %) [%]) restructured)
           (zipmap stages)
           (mapcat (fn [[stage chs]] (stage chs)))
           (#(concat % (flatten leftover)))
           vec))))

(def divvy-all
  "Supply each channel to a different stage"
  (partial divvy-by full-spread))

(defn shunt
  "Split off one or more channels and provide them to another stage"
  ([stage] (shunt stage 1))
  ([stage n] (divvy-by (partial split-at n) stage)))

(defn inject
  "Add channels. If one argument is provided, the channels are added at the end of the vector. The second argument, `which`, is a function that takes to vectors of channels and returns a vector combining them."
  ([chs] (fn [xfroms] (vec (concat xfroms chs))))
  ([chs which] (fn [xfroms] (which xfroms chs))))

;; combine or split channels

(defn mix-channels
  "Mix subsets of the input channels together. The bucketization is controlled by `which`, a function like [ch] -> [[ch]]"
  ([which] (mix-channels which async/chan))
  ([which channel-creator & channel-args]
    (fn [xfrom]
      (map (fn [from]
               (if (sequential? from)
                   (let [to (async/mix (apply channel-creator channel-args))]
                        (doseq [lil (flatten from)] (async/admix to lil))
                        to)
                   from))))))

(def mix-all
  "Mix all channels into a single channel"
  (partial mix-channels full-spread))

(defn split-channels
  "Split each channel into two based on the predicate. The first channel will contain values for which the predicate is true, the second values for which it's false. Typically only useful with one channel at a time."
  [predicate]
  (fn [xfrom]
      (mapcat #(async/split predicate xfrom))))

(defn buffer-with
  "Add a buffer to each channel"
  [xto]
  (fn [xfrom]
      (assert (= (count xto) (count xfrom)) "You must provide the same number of buffer channels as input channels")
      (doseq [[from to] (map list xfrom xto)]
        (async/pipe from to))
      xto))

(defn buffer
  "Add a buffer to each of the channels using the provided function and args for constructing each one"
  [channel-creator & channel-args]
  (fn [xfrom]
    (let [xto (new-channels xfrom #(apply channel-creator channel-args))]
         ((buffer-with xto) xfrom))))

;; todos
(defn tap [func] (fn [xfrom] nil))
(defn fork [n] (fn [xfrom] nil))
(defn parallel [n xform] nil)
(defn map-all [f] (fn [xform] nil))
(defn bury-some [which] (fn [ xform] nil))

;; start!

(defn start
  "Takes a vector of channels or function that returns a vector of channels and any number of stages to pipe through"
  [src & stages]
  ((apply chain stages) (if (fn? src) (src) src)))
