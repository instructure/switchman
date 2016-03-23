Switchman
==========

[![Build
Status](https://travis-ci.org/instructure/switchman.svg?branch=master)](https://travis-ci.org/instructure/switchman)

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

If you want to execute custom SQL upon new shard creation (i.e. to preset
default permissions), you need to set `Switchman.config[:create_statement]`,
and any instances of `%{name}` and `%{password}` will be replaced with
relevant values:

```ruby
>>> Switchman.config[:create_statement] = <<-SQL
  CREATE SCHEMA %{name};
  CREATE ROLE %{name} LOGIN %{password};
  GRANT USAGE ON SCHEMA %{name} TO readwrite;
  ALTER DEFAULT PRIVILEGES IN SCHEMA %{name} GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO readwrite;
SQL
```

If there is no `create_statement` in the config, Switchman will create a
suitable one for your database to just create a new schema (Postgres) or
database (MySQL).

To start reading/writing data on a shard, you need to activate it:

```ruby
>>> s.activate { User.create }
```

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

Switchman also disables the Rails feature where
ActionController::Base.cache_store (and other specific
MyController.cache_store values) can diverge from Rails.cache.
Controller cache's must align with Rails.cache, but now Rails.cache is
shard aware.

To take advantage of shard aware Rails.cache, simply set
config.cache_store during Rails' configuration to a hash of
configurations instead of a single configuration value. For example:

```ruby
config.cache_store = {
  'production' => [:mem_cache_store, ['memcache.cluster1'], ...],
  'cluster2'   => [:mem_cache_store, ['memcache.cluster2'], ...]
}
```

Rails.cache will then load the cache store appropriate to the current
shard's database server. If that database server does not have a cache
store defined, it will fall back to the cache store defined for the
Rails environment (e.g. config.cache_store['production']).

If the config.cache_store is a single configuration value, it will be
used as the cache store for all database servers.

## Connection Pooling

In a common Postgres situation, Switchman will switch between shards
(which are implemented as Postgres schemas/namespaces) on the same database
server by issuing a `SET search_path TO ` command prior to executing a query
against a different shard. Unfortunately, if you use pgbouncer, this means
that you cannot use transaction pooling, and must use session pooling.
There exist two workarounds to this:

### Connection Per Shard

By adding setting `username: %{shard_name},public` to database.yml,
Switchman will know that the username will vary per shard, and will establish
a new connection to the database for each shard, instead of sharing a single
connection among all shards on the same server. This will allow pgbouncer
to be set up for transaction pooling, at the cost of pgbouncer not being
able to pool connections among shards (and causing connection churn when
you start to hit pgbouncer's per-database connection limits when you
have several very active shards).

### Qualified Names

If instead you add `use_qualified_names: true` to database.yml, Switchman
will automatically prefix all table names in FROM clauses with the schema
name, like so:

```SQL
SELECT "users".* FROM "shard_11"."users"
```

Because the query no longer depends on the search_path being correct,
it's safe to use pgbouncer's transaction pooling. There are a few caveats
of this:

  * Custom SQL - if you write custom joins, you need to make sure you
    quote table names, which is how Switchman knows where to insert the
    shard name qualification:

```ruby
  User.joins("LEFT OUTER JOIN #{Appendage.quoted_table_name} ON user_id=users.id")
```

Note that you do _not_ need to modify where clauses - Postgres is smart
enough to know that shard_1.users is the only table addressed by this query,
so users.id can only possibly refer to this table. In order to enforce this
quality and prevent bugs where you forget to do this, and instead pull
data from an unexpected shard, it's recommended that you set the search_path
to something bogus. Setting `schema_search_path: "''"` in database.yml
accomplishes this.

  * Query serialized on one shard, but executed on another:

```ruby
  relation = User.joins("LEFT OUTER JOIN #{Appendage.quoted_table_name} ON user_id=users.id")
  relation.shard(@shard2).where(name: 'bob').first
```

In this case, the query will serialize as
`SELECT "users".* FROM "shard_2"."users" LEFT OUTER JOIN "shard_1"."appendages" ...`,
causing a cross-shard query. If the two shards are on separate database
servers, it will simply fail. This happens because the JOIN serialized
while shard 1 was active, but the query executed (and the initial FROM)
against shard 2. This could also happen with a subquery:

```ruby
  relation = Appendage.all
  @shard1.activate { relation.where("EXISTS (?)", User.where(name: 'bob')) }
  relation.first
```

In this case, the subquery was serialized at the `where` call, and not
delayed until actual query execution. The solution to both of these
problems is you must be careful around such queries, to ensure the
serialization happens on the correct shard.
