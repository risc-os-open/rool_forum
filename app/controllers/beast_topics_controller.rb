class BeastTopicsController < ActionController::Base
  def show
    topic = Thredded::Topic.find( params[ :id ] )
    redirect_to(
      thredded_path + thredded.messageboard_topic_path( {
        :messageboard_id => params[ :messageboard_id ],
        :id              => topic.slug
      } ),
      { :status => :moved_permanently }
    )
  end
end
