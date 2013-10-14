Dummy::Application.routes.draw do
  resources :users do
    resources :appendages
  end

  match 'users/:id' => 'users#test1', :as => :user_test1
  match 'users/:user_id/test2' => 'users#test2', :as => :user_test2
end