class HomeController < ActionController::Base
  def show
    redirect_to root_url + 'forums', :status => :moved_permanently
  end
end
