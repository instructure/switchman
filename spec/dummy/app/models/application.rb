# frozen_string_literal: true

class Application < Switchman::UnshardedRecord
  belongs_to :root, :required => false
end
