require "spec_helper"

module Switchman
  module ActiveRecord
    describe Calculations do
      include RSpecHelper

      describe "#pluck" do
        before do
          @shard1.activate do
            @user1 = User.create!(:name => "user1")
            @appendage1 = @user1.appendages.create!
          end
          @shard2.activate do
            @user2 = User.create!(:name => "user2")
            @appendage2 = @user2.appendages.create!
          end
        end

        it "should return non-id columns" do
          User.where(:id => [@user1.id, @user2.id]).pluck(:name).sort.should == ["user1", "user2"]
        end

        it "should return primary ids relative to current shard" do
          Appendage.where(:id => @appendage1).pluck(:id).should == [@appendage1.global_id]
          Appendage.where(:id => @appendage2).pluck(:id).should == [@appendage2.global_id]
          @shard1.activate do
            Appendage.where(:id => @appendage1).pluck(:id).should == [@appendage1.local_id]
            Appendage.where(:id => @appendage2).pluck(:id).should == [@appendage2.global_id]
          end
          @shard2.activate do
            Appendage.where(:id => @appendage1).pluck(:id).should == [@appendage1.global_id]
            Appendage.where(:id => @appendage2).pluck(:id).should == [@appendage2.local_id]
          end
        end

        it "should return foreign ids relative to current shard" do
          Appendage.where(:id => @appendage1).pluck(:user_id).should == [@user1.global_id]
          Appendage.where(:id => @appendage2).pluck(:user_id).should == [@user2.global_id]
          @shard1.activate do
            Appendage.where(:id => @appendage1).pluck(:user_id).should == [@user1.local_id]
            Appendage.where(:id => @appendage2).pluck(:user_id).should == [@user2.global_id]
          end
          @shard2.activate do
            Appendage.where(:id => @appendage1).pluck(:user_id).should == [@user1.global_id]
            Appendage.where(:id => @appendage2).pluck(:user_id).should == [@user2.local_id]
          end
        end
      end

      describe "#execute_simple_calculation" do
        before do
          @appendages = []
          @shard1.activate do
            @user1 = User.create!(:name => "user1")
            @appendages << @user1.appendages.create!(:value => 1)
            @appendages << @user1.appendages.create!(:value => 2)
          end
          @shard2.activate do
            @user2 = User.create!(:name => "user2")
            @appendages << @user2.appendages.create!(:value => 3)
            @appendages << @user2.appendages.create!(:value => 4)
            @appendages << @user2.appendages.create!(:value => 5)
          end
        end

        it "should calculate average across shards" do
          @user1.appendages.average(:value).should == 1.5
          @shard1.activate {Appendage.average(:value)}.should == 1.5

          @user2.appendages.average(:value).should == 4
          @shard2.activate {Appendage.average(:value)}.should == 4

          Appendage.where(:id => @appendages).average(:value).should == 3
        end

        it "should count across shards" do
          @user1.appendages.count.should == 2
          @shard1.activate {Appendage.count}.should == 2

          @user2.appendages.count.should == 3
          @shard2.activate {Appendage.count}.should == 3

          Appendage.where(:id => @appendages).count.should == 5
        end

        it "should calculate minimum across shards" do
          @user1.appendages.minimum(:value).should == 1
          @shard1.activate {Appendage.minimum(:value)}.should == 1

          @user2.appendages.minimum(:value).should == 3
          @shard2.activate {Appendage.minimum(:value)}.should == 3

          Appendage.where(:id => @appendages).minimum(:value).should == 1
        end

        it "should calculate maximum across shards" do
          @user1.appendages.maximum(:value).should == 2
          @shard1.activate {Appendage.maximum(:value)}.should == 2

          @user2.appendages.maximum(:value).should == 5
          @shard2.activate {Appendage.maximum(:value)}.should == 5

          Appendage.where(:id => @appendages).maximum(:value).should == 5
        end

        it "should calculate sum across shards" do
          @user1.appendages.sum(:value).should == 3
          @shard1.activate {Appendage.sum(:value)}.should == 3

          @user2.appendages.sum(:value).should == 12
          @shard2.activate {Appendage.sum(:value)}.should == 12

          Appendage.where(:id => @appendages).sum(:value).should == 15
        end
      end

      describe "#execute_grouped_calculation" do
        before do
          @appendages = []
          @shard1.activate { @user1 = User.create!(:name => "user1") }
          @shard2.activate { @user2 = User.create!(:name => "user2") }

          @shard1.activate do
            @appendages << Appendage.create!(:user_id => @user1.id, :value => 1)
            @appendages << Appendage.create!(:user_id => @user2.id, :value => 3)
          end
          @shard2.activate do
            @appendages << Appendage.create!(:user_id => @user1.id, :value => 2)
            @appendages << Appendage.create!(:user_id => @user2.id, :value => 4)
            @appendages << Appendage.create!(:user_id => @user2.id, :value => 5)
          end
        end

        it "should calculate average across shards" do
          Appendage.shard([@shard1, @shard2]).group("appendages.user_id").average(:value).should ==
              {@user1.global_id => 1.5, @user2.global_id => 4}

          @shard1.activate do
            Appendage.shard([@shard1, @shard2]).group("appendages.user_id").average(:value).should ==
                {@user1.local_id => 1.5, @user2.global_id => 4}
          end

          @shard2.activate do
            Appendage.shard([@shard1, @shard2]).group("appendages.user_id").average(:value).should ==
                {@user1.global_id => 1.5, @user2.local_id => 4}
          end

          Appendage.shard([@shard1, @shard2]).group(:user).average(:value).should ==
              {@user1 => 1.5, @user2 => 4}
        end

        it "should count across shards" do
          Appendage.shard([@shard1, @shard2]).group("appendages.user_id").count.should ==
              {@user1.global_id => 2, @user2.global_id => 3}

          @shard1.activate do
            Appendage.shard([@shard1, @shard2]).group("appendages.user_id").count.should ==
                {@user1.local_id => 2, @user2.global_id => 3}
          end

          @shard2.activate do
            Appendage.shard([@shard1, @shard2]).group("appendages.user_id").count.should ==
                {@user1.global_id => 2, @user2.local_id => 3}
          end

          Appendage.shard([@shard1, @shard2]).group(:user).count.should ==
              {@user1 => 2, @user2 => 3}
        end

        it "should calculate minimum across shards" do
          Appendage.shard([@shard1, @shard2]).group("appendages.user_id").minimum(:value).should ==
              {@user1.global_id => 1, @user2.global_id => 3}

          @shard1.activate do
            Appendage.shard([@shard1, @shard2]).group("appendages.user_id").minimum(:value).should ==
                {@user1.local_id => 1, @user2.global_id => 3}
          end

          @shard2.activate do
            Appendage.shard([@shard1, @shard2]).group("appendages.user_id").minimum(:value).should ==
                {@user1.global_id => 1, @user2.local_id => 3}
          end

          Appendage.shard([@shard1, @shard2]).group(:user).minimum(:value).should ==
              {@user1 => 1, @user2 => 3}
        end

        it "should calculate maximum across shards" do
          Appendage.shard([@shard1, @shard2]).group("appendages.user_id").maximum(:value).should ==
              {@user1.global_id => 2, @user2.global_id => 5}

          @shard1.activate do
            Appendage.shard([@shard1, @shard2]).group("appendages.user_id").maximum(:value).should ==
                {@user1.local_id => 2, @user2.global_id => 5}
          end

          @shard2.activate do
            Appendage.shard([@shard1, @shard2]).group("appendages.user_id").maximum(:value).should ==
                {@user1.global_id => 2, @user2.local_id => 5}
          end

          Appendage.shard([@shard1, @shard2]).group(:user).maximum(:value).should ==
              {@user1 => 2, @user2 => 5}
        end

        it "should calculate sum across shards" do
          Appendage.shard([@shard1, @shard2]).group("appendages.user_id").sum(:value).should ==
              {@user1.global_id => 3, @user2.global_id => 12}

          @shard1.activate do
            Appendage.shard([@shard1, @shard2]).group("appendages.user_id").sum(:value).should ==
                {@user1.local_id => 3, @user2.global_id => 12}
          end

          @shard2.activate do
            Appendage.shard([@shard1, @shard2]).group("appendages.user_id").sum(:value).should ==
                {@user1.global_id => 3, @user2.local_id => 12}
          end

          Appendage.shard([@shard1, @shard2]).group(:user).sum(:value).should ==
              {@user1 => 3, @user2 => 12}
        end
      end
    end
  end
end
