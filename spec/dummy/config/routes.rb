# frozen_string_literal: true

Dummy::Application.routes.draw do
  resources :users do
    resources :appendages
  end

  get 'users/:id' => 'users#test1', :as => :user_test1
  get 'users/:user_id/test2' => 'users#test2', :as => :user_test2
end
