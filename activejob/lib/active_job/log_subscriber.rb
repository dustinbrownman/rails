# frozen_string_literal: true

require "active_support/log_subscriber"

module ActiveJob
  class LogSubscriber < ActiveSupport::LogSubscriber # :nodoc:
    class_attribute :backtrace_cleaner, default: ActiveSupport::BacktraceCleaner.new

    def enqueue(event)
      job = event.payload[:job]
      ex = event.payload[:exception_object] || job.enqueue_error

      if ex
        error do
          "Failed enqueuing #{job_info(event, include_job_id: false)} to #{queue_name(event)}: #{exception_info(ex)}"
        end
      elsif event.payload[:aborted]
        info do
          "Failed enqueuing #{job_info(event, include_job_id: false)} to #{queue_name(event)}, a before_enqueue callback halted the enqueuing execution."
        end
      else
        info do
          "Enqueued #{job_info(event)} to #{queue_name(event)} #{args_info(event)}".squeeze(" ")
        end
      end
    end
    subscribe_log_level :enqueue, :info

    def enqueue_at(event)
      job = event.payload[:job]
      ex = event.payload[:exception_object] || job.enqueue_error

      if ex
        error do
          "Failed enqueuing #{job_info(event, include_job_id: false)} to #{queue_name(event)}: #{exception_info(ex)}"
        end
      elsif event.payload[:aborted]
        info do
          "Failed enqueuing #{job_info(event, include_job_id: false)} to #{queue_name(event)}, a before_enqueue callback halted the enqueuing execution."
        end
      else
        info do
          "Enqueued #{job_info(event)} to #{queue_name(event)} at #{scheduled_at(event)} #{args_info(event)}".squeeze(" ")
        end
      end
    end
    subscribe_log_level :enqueue_at, :info

    def enqueue_all(event)
      info do
        jobs = event.payload[:jobs]
        adapter = event.payload[:adapter]
        enqueued_count = event.payload[:enqueued_count]

        if enqueued_count == jobs.size
          enqueued_jobs_message(adapter, jobs)
        elsif jobs.any?(&:successfully_enqueued?)
          enqueued_jobs = jobs.select(&:successfully_enqueued?)

          failed_enqueue_count = jobs.size - enqueued_count
          if failed_enqueue_count == 0
            enqueued_jobs_message(adapter, enqueued_jobs)
          else
            "#{enqueued_jobs_message(adapter, enqueued_jobs)}. "\
              "Failed enqueuing #{failed_enqueue_count} #{'job'.pluralize(failed_enqueue_count)}"
          end
        else
          failed_enqueue_count = jobs.size - enqueued_count
          "Failed enqueuing #{failed_enqueue_count} #{'job'.pluralize(failed_enqueue_count)} "\
            "to #{ActiveJob.adapter_name(adapter)}"
        end
      end
    end
    subscribe_log_level :enqueue_all, :info

    def perform_start(event)
      info do
        "Performing #{job_info(event)} from #{enqueue_info(event)} #{args_info(event)}".squeeze(" ")
      end
    end
    subscribe_log_level :perform_start, :info

    def perform(event)
      ex = event.payload[:exception_object]
      if ex
        error do
          "Error performing #{job_info(event)} from #{queue_name(event)} in #{duration(event)}: #{exception_info(ex, include_backtrace: true)}"
        end
      elsif event.payload[:aborted]
        error do
          "Error performing #{job_info(event)} from #{queue_name(event)} in #{duration(event)}: a before_perform callback halted the job execution"
        end
      else
        info do
          "Performed #{job_info(event)} from #{queue_name(event)} in #{duration(event)}"
        end
      end
    end
    subscribe_log_level :perform, :info

    def enqueue_retry(event)
      ex = event.payload[:error]

      info do
        if ex
          "Retrying #{job_info(event)} after #{attempts(event)} in #{wait_time(event)}, due to a #{exception_info(ex)}."
        else
          "Retrying #{job_info(event)} after #{attempts(event)} in #{wait_time(event)}."
        end
      end
    end
    subscribe_log_level :enqueue_retry, :info

    def retry_stopped(event)
      ex = event.payload[:error]

      error do
        "Stopped retrying #{job_info(event)} due to a #{exception_info(ex)}, which reoccurred on #{attempts(event)}."
      end
    end
    subscribe_log_level :enqueue_retry, :error

    def discard(event)
      ex = event.payload[:error]

      error do
        "Discarded #{job_info(event)} due to a #{exception_info(ex)}."
      end
    end
    subscribe_log_level :discard, :error

    private
      def queue_name(event)
        ActiveJob.adapter_name(event.payload[:adapter]) + "(#{event.payload[:job].queue_name})"
      end

      def enqueue_info(event)
        return "" unless event.payload[:job].enqueued_at.present?

        "#{queue_name(event)} enqueued at #{enqueued_at(event)}"
      end

      def job_info(event, include_job_id: true)
        job = event.payload[:job]

        if include_job_id
          "#{job.class.name} (Job ID: #{job.job_id})"
        else
          job.class.name
        end
      end

      def duration(event)
        "#{event.duration.round(2)}ms"
      end

      def wait_time(event)
        "#{event.payload[:wait].to_i} seconds"
      end

      def attempts(event)
        "#{event.payload[:job].executions} attempts"
      end

      def scheduled_at(event)
        format_time(event.payload[:job].scheduled_at)
      end

      def enqueued_at(event)
        format_time(event.payload[:job].enqueued_at)
      end

      def args_info(event)
        job = event.payload[:job]
        return "" unless job.class.log_arguments? && job.arguments.any?

        "with arguments: " + job.arguments.map { |arg| format(arg).inspect }.join(", ")
      end

      def format(arg)
        case arg
        when Hash
          arg.transform_values { |value| format(value) }
        when Array
          arg.map { |value| format(value) }
        when GlobalID::Identification
          arg.to_global_id rescue arg
        else
          arg
        end
      end

      def format_time(time)
        Time.at(time).utc.iso8601(9)
      end

      def exception_info(ex, include_backtrace: false)
        ex_info = "#{ex.class} (#{ex.message})"
        ex_info += ":\n#{ex.backtrace.join("\n")}" if include_backtrace && ex.backtrace.present?

        ex_info
      end

      def logger
        ActiveJob::Base.logger
      end

      def info(progname = nil, &block)
        return unless super

        if ActiveJob.verbose_enqueue_logs
          log_enqueue_source
        end
      end

      def error(progname = nil, &block)
        return unless super

        if ActiveJob.verbose_enqueue_logs
          log_enqueue_source
        end
      end

      def log_enqueue_source
        source = extract_enqueue_source_location(caller)

        if source
          logger.info("↳ #{source}")
        end
      end

      def extract_enqueue_source_location(locations)
        backtrace_cleaner.clean(locations.lazy).first
      end

      def enqueued_jobs_message(adapter, enqueued_jobs)
        enqueued_count = enqueued_jobs.size
        job_classes_counts = enqueued_jobs.map(&:class).tally.sort_by { |_k, v| -v }
        "Enqueued #{enqueued_count} #{'job'.pluralize(enqueued_count)} to #{ActiveJob.adapter_name(adapter)}"\
          " (#{job_classes_counts.map { |klass, count| "#{count} #{klass}" }.join(', ')})"
      end
  end
end

ActiveJob::LogSubscriber.attach_to :active_job
