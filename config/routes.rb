Rails.application.routes.draw do
  root to: 'home#show'
  devise_for :users
  # For details on the DSL available within this file, see http://guides.rubyonrails.org/routing.html
  resources :users, only: [:show]
  mount Thredded::Engine => '/forums'

  get '/forums/:messageboard_id/topics/:id', :to => 'beast_topics#show'
end
