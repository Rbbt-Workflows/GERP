require 'rbbt-util'
require 'rbbt/resource'

$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '../../..', 'lib'))

module GERP
  extend Resource
  self.subdir = 'var/GERP'

  GERP.claim GERP.data, :proc do |directory|
    url = "http://mendel.stanford.edu/SidowLab/downloads/gerp/hg19.GERP_scores.tar.gz"
    io = Open.open(url, :nocache => true)
    Misc.untar(io, directory.find)
    nil
  end

  GM_SHARD_FUNCTION = Proc.new do |key|
    key[0..key.index(":")-1]
  end

  CHR_POS = Proc.new do |key|
    raise "Key (position) not String: #{ key }" unless String === key
    if match = key.match(/.*?:(\d+):?/)
      match[1].to_i
    else
      raise "Key (position) not understood: #{ key }"
    end
  end

  def self.database
    @@database ||= begin
                     Persist.persist_tsv("GERP", GERP.data.find, {}, :persist => true,
                                         :file => GERP.scores_packed_shard.find,
                                         :prefix => "GERP", :pattern => %w(f f), :engine => "pki",
                                         :shard_function => GM_SHARD_FUNCTION, :pos_function => CHR_POS) do |sharder|

                       sharder.fields = ["gerpcol", "gerpelem"]
                       sharder.key_field = "Genomic Position"
                       sharder.type = :list

                       files = GERP.data.glob('*.rates').sort
                       TSV.traverse files do |file|
                         chr = File.basename(file).split('.').first.sub(/chr/,'')

                         db = sharder.database(chr + ':')
                         TSV.traverse file, :type => :array, :into => db, :bar => File.basename(file) do |line|
                           v1, _sep, v2 = line.partition "\t"
                           [v1.to_f,v2.to_f]
                         end
                       end
                      end
                    end
  end
end

if __FILE__ == $0
  GERP.database
end
