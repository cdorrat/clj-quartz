(ns ^{:author "Paul Ingles"
      :doc "Functions to creating and controlling Quartz Schedulers."}
  clj-quartz.scheduler
  (:require [clj-quartz [job :as job]]
	    [clj-time
	     [coerce :as tc]
	     [core :as t]]
	    )
  (:import [org.quartz.impl StdSchedulerFactory]
           [org.quartz Scheduler JobDetail Trigger JobKey JobDataMap]
           [org.quartz.impl.triggers SimpleTriggerImpl CronTriggerImpl]
           [org.quartz.impl.matchers GroupMatcher]
	   [org.joda.time Duration Interval] )
  (:use [clj-quartz.core :only (as-properties)]))

(def default-quartz-props {"org.quartz.scheduler.instanceName" "clj-quartz"
			   "org.quartz.threadPool.threadCount" "2"
			   "org.quartz.jobStore.class" "org.quartz.simpl.RAMJobStore"})

(defn create-scheduler
  "Creates a scheduler with the provided configuration (a sequence of
   property name/values)."
  ([] (create-scheduler default-quartz-props))
  ([config]
     (.getScheduler (doto (StdSchedulerFactory.)
		      (.initialize (as-properties (merge default-quartz-props config)))))))

(defn start
  [^Scheduler scheduler]
  (.start scheduler))

(defn shutdown
  [^Scheduler scheduler]
  (.shutdown scheduler))

(defn standby
  "Temporarily halts the firing of triggers."
  [^Scheduler scheduler]
  (.standby scheduler))

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
  "Schedules the job for execution. Provide a trigger to create
   trigger now."
  ([^Scheduler scheduler ^Trigger trigger]
     (.scheduleJob scheduler trigger))
  ([^Scheduler scheduler ^JobDetail job ^Trigger trigger]
     (.scheduleJob scheduler job trigger)))

(defn add-job
  "Adds a job ready for scheduling later."
  [^Scheduler scheduler ^JobDetail job & {:keys [replace] :or {replace true}}]
  (.addJob scheduler job replace))

(defn delete-job
  [^Scheduler scheduler {:keys [group name]}]
  (.deleteJob scheduler (JobKey. name group)))

(defn trigger
  "Schedules a previously added job with specified trigger."
  ([^Scheduler scheduler ^JobKey job-key m]
     (.triggerJob scheduler job-key (JobDataMap. m)))
  ([^Scheduler scheduler ^JobKey job-key]
     (.triggerJob scheduler job-key)))

(defn group-names
  [^Scheduler scheduler]
  (.getJobGroupNames scheduler))

(defn job-keys
  [^Scheduler scheduler group-name]
  (map (fn [x] {:group (.getGroup x)
               :name (.getName x)})
       (.getJobKeys scheduler (GroupMatcher/groupContains group-name))))


(defn- job-detail-to-map [^JobDetail detail]
  (let [^JobKey k (.getKey detail)]
    {:name (.getName k)
     :group (.getGroup k)
     :description (.getDescription detail)
     :data (.getJobDataMap detail)
     :durable? (.isDurable detail)
     :persist-data-after-execution? (.isPersistJobDataAfterExecution detail)
     :requests-recovery? (.requestsRecovery detail)
     :concurrent-execuion-allowed? (not (.isConcurrentExectionDisallowed detail))}
     ))
     
  

(defn get-job-detail [scheduler job-name group]
  (.getJobDetail scheduler (JobKey. job-name group)))

(defn delete-job [scheduler {:keys [name group]}]
  (.deleteJob scheduler (JobKey. name group)))

(defn- time-field-to-msecs [fld]
  (if (number? fld)
    fld
    (t/in-msecs
     (Interval. (t/now) (.. fld toPeriod toStandardDuration)))))

(defn- get-or-create-job [scheduler job-or-fn job-name group data]
  (if (string? job-or-fn)
    (if-let [details (get-job-detail job-name group)]
      details
      (throw (RuntimeException. (format "The job %s (group: %s) does not exist" job-or-fn group))))
    (let [job (job/create-job-detail {:name job-name :group group :data data :f job-or-fn})]
      (add-job scheduler job)
      job)))
      
         
(defn- to-date [date-or-datetime]  
  (cond
   (nil? date-or-datetime) (java.util.Date.)
   (instance? java.util.Date date-or-datetime) date-or-datetime
   :else (tc/to-date date-or-datetime)))
  
(defn- get-job-data [scheduler job-or-fn job-name group data]
  (let  [name (or (when (string? job-or-fn) job-or-fn) job-name (str (gensym "clj-quartz")))
	 group (or group "clj-quartz")]
    [name group (get-or-create-job scheduler job-or-fn name group data)]))
  
(defn schedule-cron
  "Schedule a recurring job using cron syntax"
  [scheduler cron-spec job-or-fn & {:keys [name group data]}]
  (let [[name group job-detail] (get-job-data scheduler job-or-fn name group data)]
    (schedule scheduler
		(doto (CronTriggerImpl.)
		  (.setName (str (gensym "cron-")))
		  (.setGroup group)
		  (.setJobName name)
		  (.setJobGroup group)
		  (.setCronExpression cron-spec)))
    job-detail))    
  

(defn schedule-repeatedly
  "Schedule a job to run periodically at fixed intervals"
  [scheduler first-run period num-repeats job-or-fn & {:keys [name group data]}]
  (let [[name group job-detail] (get-job-data scheduler job-or-fn name group data)]		 
    (schedule scheduler
		(doto (SimpleTriggerImpl. )
		  (.setName (str (gensym "trigger")))
		  (.setGroup group)
		  (.setJobName name)
		  (.setJobGroup group)
		  (.setRepeatCount num-repeats)
		  (.setRepeatInterval (time-field-to-msecs period))
		  (.setStartTime (to-date first-run))))
    job-detail))
    
(defn schedule-at
  "Schedule a job to run once at a specified time"
  [scheduler instant job-or-fn & {:keys [name group data]}]
  (schedule-repeatedly scheduler instant 0 0 job-or-fn :job-name name :group group :data data))

(defn- trigger-to-map [trigger]
  (let [key (.getKey trigger)]    
    {:trigger-name (.getName key)
     :trigger-group (.getGroup key)
     :start-time (.getStartTime trigger)
     :end-time (.getEndTime trigger)
     :prev-fire-time (.getPreviousFireTime trigger)
     :next-fire-time (.getNextFireTime trigger)
     :final-fire-time (.getFinalFireTime trigger)}))
   
(defn list-jobs
  ([scheduler]
     (let [all-groups (group-names scheduler)]
       (if-not (empty? all-groups)
	 (apply list-jobs scheduler all-groups)
	 (list))))
  ([scheduler & groups]
     (mapcat (fn [{:keys [name group]}]
	       (let [job-detail (get-job-detail scheduler name group)
		     detail-map (job-detail-to-map job-detail)
		     triggers (.getTriggersOfJob scheduler (.getKey job-detail))]
		 (if (empty? triggers)
		   [detail-map]
		   (map #(merge detail-map (trigger-to-map %)) triggers))))
	     (mapcat #(job-keys scheduler %) groups))))
