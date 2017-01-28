# frozen_string_literal: true
require 'redcloth'

module Thredded
  module HtmlPipeline
    class TextileFilter < ::HTML::Pipeline::TextFilter
      def initialize(text, context = nil, result = nil)
        super text, context, result
        @text.delete! "\r"
      end

      def call
        result = RedCloth.new(@text).to_html
        result.rstrip!
        result
      end
    end
  end
end
