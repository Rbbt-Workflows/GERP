require 'rbbt-util'
require 'rbbt/workflow'

require 'rbbt/sources/GERP'

module GERP
  extend Workflow

  class << self
    attr_accessor :organism
  end

  self.organism = "Hsa/jan2013"

  input :mutations, :array, "Genomic Mutation", nil, :stream => true
  task :annotate => :tsv do |mutations|
    database = GERP.database
    database.unnamed = true
    dumper = TSV::Dumper.new :key_field => "Genomic Mutation", :fields => database.fields, :type => :list, :cast => :to_f
    dumper.init
    TSV.traverse mutations, :into => dumper, :bar => self.progress_bar("Annotate with GERP"), :type => :array do |mutation|
      next if mutation.empty?
      p = database[mutation]
      next if p.nil?
      [mutation, p]
    end
  end
end
