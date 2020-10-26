# frozen_string_literal: true

require 'open4'

# This fixes a bug with exception handling,
# see https://github.com/ahoward/open4/pull/30
module Open4
  def self.do_popen(b = nil, exception_propagation_at = nil, closefds=false, &cmd)
    pw, pr, pe, ps = IO.pipe, IO.pipe, IO.pipe, IO.pipe

    verbose = $VERBOSE
    begin
      $VERBOSE = nil

      cid = fork {
        if closefds
          exlist = [0, 1, 2] | [pw,pr,pe,ps].map{|p| [p.first.fileno, p.last.fileno] }.flatten
          ObjectSpace.each_object(IO){|io|
            io.close if (not io.closed?) and (not exlist.include? io.fileno) rescue nil
          }
        end

        pw.last.close
        STDIN.reopen pw.first
        pw.first.close

        pr.first.close
        STDOUT.reopen pr.last
        pr.last.close

        pe.first.close
        STDERR.reopen pe.last
        pe.last.close

        STDOUT.sync = STDERR.sync = true

        begin
          cmd.call(ps)
        rescue Exception => e
          begin
            Marshal.dump(e, ps.last)
            ps.last.flush
          rescue Errno::EPIPE
            raise e
          end
        ensure
          ps.last.close unless ps.last.closed?
        end

        exit!
      }
    ensure
      $VERBOSE = verbose
    end

    [ pw.first, pr.last, pe.last, ps.last ].each { |fd| fd.close }

    Open4.propagate_exception cid, ps.first if exception_propagation_at == :init

    pw.last.sync = true

    pi = [ pw.last, pr.first, pe.first ]

    begin
      return [cid, *pi] unless b

      begin
        b.call(cid, *pi)
      ensure
        pi.each { |fd| fd.close unless fd.closed? }
      end

      Open4.propagate_exception cid, ps.first if exception_propagation_at == :block

      Process.waitpid2(cid).last
    ensure
      ps.first.close unless ps.first.closed?
    end
  end
end
