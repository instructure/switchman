Switchman
==========

## WARNING

Switchman is being ported from private code used in Instructure's canvas-lms
product. This port is not yet complete, so there may be missing functionality
or bugs still remaining.

## About

Switchman is an ActiveRecord extension for sharding your database. Key features
include:

 * Integration with the Shackles gem for master/slave/deploy environments
 * Creation of shards on the fly
 * Multiple shard "categories"
 * Support for cross-shard references in associations (and even multi-shard
   associations)
 * Hooking of ActiveRecord querying infrastructure to automatically infer the
   correct shard to execute the query on, and to perform necessary
   translations of foreign keys
 * Support multiple shards on the same database server (using Schemas on
   PostgreSQL)
 * Primarily supports PostgreSQL, but MySQL should work. Please report any bugs
   on MySQL!

## Installation

Add `gem 'switchman'` to your Gemfile (requires Rails 3.2 only)

## Usage

With Switchman, database servers are defined in database.yml, and shards are
defined in a table on the "default" shard. The default shard is the database
that is defined by the test/development/production block in database.yml.

Example database.yml file:

```yaml
production:
  adapter: postgresql
  host: db1

cluster2:
  adapter: postgresql
  host: db2
```

To create a new shard, you find the database server, and call `create_new_shard`:

```ruby
>>> s = Switchman::Shard.default.database_server.create_new_shard
>>> s = Switchman::DatabaseServer.find('cluster2').create_new_shard
```

To start reading/writing data on a shard, you need to activate it:

```ruby
>>> s.activate { User.create }

## IDs

Each shard has a shard id, the primary key in the switchman_shards table. This
shard id is used so that data in one shard can reference data in another shard.
It does this by adding the shard id multiplied by 10 trillion to the id of the
item in that shard. This is called the "global id". For example, a user in
shard 1 with a "local id" of 1 has a global id of 10000000000001. This way, we
can determine what shard any given id is on by doing some math (either a
specific shard, or the "current" shard if it's less than 10 trillion). For
convenience purposes, most Switchman methods also understand "short global ids",
which is simply the shard id, a tilde, and then the local id (1~1).

Also, a foreign key can reference an object in a different shard by using its
global id. Because of this, all foreign (and primary!) keys in the database
should be 64-bit integers. Assuming that the database uses signed integers, so
that we have 63 rather than 64 bits to use for the ids, you have
(2^63)/(10 trillion)=922,337 shards available.

## Categories

Categories let you activate different shards for different purposes at the same
time. There are two special categories. A model marked as belonging to the
`unsharded` category always lives on the default shard. An example of this is
the switchman_shards table itself - there is only one copy of the data in this
table, no matter how many shards there are. The other is `default`, which is
every model not otherwise marked as belonging to a category. You can come up
with your own categories by simply annotating your models, like so:

```ruby
class SomeModel < ActiveRecord::Base
  self.shard_category = :some_category
end
```

When activating shards, the `unsharded` category is always locked to the
default shard, and passing no arguments defaults to `default`:

```ruby
>>> Switchman::Shard.current(:default)
 => <Switchman::Shard id:1>
>>> Switchman::Shard.current
 => <Switchman::Shard id:1>
>>> shard2.activate { Switchman::Shard.current }
 => <Switchman::Shard id:2>
>>> shard2.activate(:some_category) {
 [Switchman::Shard.current, Switchman::Shard.current(:some_category)] }
 => [<Switchman::Shard id:1>, <Switchman::Shard id:2>]
>>> Switchman::Shard.activate(default: shard2, some_category: shard3) {
  [Switchman::Shard.current, Switchman::Shard.current(:some_category)] }
 => [<Switchman::Shard id:2>, <Switchman::Shard id:3>]
```

## Rails extensions

Relations are extended to know which shard their query will execute on, to
infer the shard from primary key conditions, and to alter queries
appropriately when the shard is changed:

```ruby
>>> User.where(id: "2~1").shard_value
 => <Switchman::Shard id:2>
>>> User.where(id: "2~1").to_sql
 => "SELECT * FROM users WHERE id=1"
>>> Appendage.where(user_id: 1).shard(shard2).to_sql
 => "SELECT * FROM appendages WHERE user_id=10000000000001"
```

Associations automatically set their shard to the owning record, which then
executes the query on that record's shard:

```ruby
>>> User.first.appendages.shard_value
 => <User id:1>
```

ActiveRecord objects used to generate URLs automatically use short global IDs
in the URL to reduce user confusion with long URLs:

```ruby
>>> polymorphic_path([User.first])
 => "/users/1"
>>> polypmorphic_path([shard2.activate { User.first }])
 => "/users/2~1"
```