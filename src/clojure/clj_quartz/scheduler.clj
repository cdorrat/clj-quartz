(ns ^{:author "Paul Ingles"
      :doc "Functions to creating and controlling Quartz Schedulers."}
  clj-quartz.scheduler
  (:import [org.quartz.impl StdSchedulerFactory]
           [org.quartz Scheduler JobDetail Trigger]
           [org.quartz.impl.triggers SimpleTriggerImpl]
           [org.quartz.impl.matchers GroupMatcher])
  (:use [clj-quartz.core :only (as-properties)]))

(defn create-scheduler
  "Creates a scheduler with the provided configuration (a sequence of
   property name/values)."
  [config]
  (.getScheduler (doto (StdSchedulerFactory.)
                   (.initialize (as-properties config)))))

(defn start
  [^Scheduler scheduler]
  (.start scheduler))

(defn shutdown
  [^Scheduler scheduler]
  (.shutdown scheduler))

(defn metadata
  [^Scheduler scheduler]
  (let [data (.getMetaData scheduler)]
    {:job-store (.getJobStoreClass data)
     :scheduler (.getSchedulerClass data)
     :instance-id (.getSchedulerInstanceId data)
     :name (.getSchedulerName data)
     :summary (.getSummary data)
     :thread-pool (.getThreadPoolClass data)
     :thread-pool-size (.getThreadPoolSize data)
     :version (.getVersion data)
     :standby-mode (.isInStandbyMode data)
     :shutdown (.isShutdown data)
     :started (.isStarted data)}))

(defn started?
  [scheduler]
  (:started (metadata scheduler)))


;; TODO
;; change interface of schedule so it works with the description
;; of jobs + data from the job ns.
;;
;; in the meantime, create the Quartz objects based on the map
;; described in job.

(defn schedule
  "Schedules the job for execution."
  [^Scheduler scheduler ^JobDetail job ^Trigger trigger]
  (.scheduleJob scheduler job trigger))

(defn add-job
  "Adds a job ready for scheduling later."
  [^Scheduler scheduler ^JobDetail job & {:keys [replace] :or {replace true}}]
  (.addJob scheduler job replace))

(defn group-names
  [^Scheduler scheduler]
  (.getJobGroupNames scheduler))

(defrecord JobKey [group name])

(defn job-keys
  [^Scheduler scheduler group-name]
  (map (fn [x] (JobKey. (.getGroup x) (.getName x)))
       (.getJobKeys scheduler (GroupMatcher/groupContains group-name))))