Switchman
==========

[![Build
Status](https://travis-ci.org/instructure/switchman.svg?branch=main)](https://travis-ci.org/instructure/switchman)

## About

Switchman is an ActiveRecord extension for sharding your database. Key features
include:

 * Integration with the GuardRail gem for primary/secondary/deploy environments
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

## Requirements

* Ruby 2.4+
* Rails 5.1+

## Installation

1. Add `gem 'switchman'` to your Gemfile
2. Run `bundle install` from your project's root
3. Run `rake switchman:install:migrations` to copy over the migration files
   from Switchman to your application

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

## Vertical Sharding

Vertical sharding lets you activate different shards for different purposes at
the same time. There are two special classes. A model inheriting from
Switchman::UnshardedRecord always lives on the default shard. An example of this is
the switchman_shards table itself - there is only one copy of the data in this
table, no matter how many shards there are. The other is `ActiveRecord::Base`, which is
every model not otherwise inheriting from a connection class. You can come up
with your own verticals by creating an abstract class, and inheriting from it,
like so:

```ruby
class SomeAbstractModel < ActiveRecord::Base
  sharded_model
end

class SomeModel < SomeAbstractModel
end
```

When activating shards, the `Switchman::UnshardedRecord` model is always locked to the
default shard, and passing no arguments defaults to `ActiveRecord::Base`:

```ruby
>>> Switchman::Shard.current(ActiveRecord::Base)
 => <Switchman::Shard id:1>
>>> Switchman::Shard.current
 => <Switchman::Shard id:1>
>>> shard2.activate { Switchman::Shard.current }
 => <Switchman::Shard id:2>
>>> shard2.activate(SomeAbstractModel) {
 [Switchman::Shard.current, Switchman::Shard.current(SomeAbstractModel)] }
 => [<Switchman::Shard id:1>, <Switchman::Shard id:2>]
>>> Switchman::Shard.activate(ActiveRecord::Base => shard2, SomeAbstractModel => shard3) {
  [Switchman::Shard.current, Switchman::Shard.current(SomeAbstractModel)] }
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

### Qualified Names

Switchman will automatically prefix all table names in FROM clauses with the schema
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

## A Note on Shard Naming

When using Postgres, it may be tempting to have your default shard (or even
other shards on other database servers) be named 'public'. In fact, this
is likely to happen silently if you just start using switchman in a new
database, because Postgres defaults the schema_search_path to
"${user},public", and creates a public schema, but likely not a schema
corresponding to the username you're using to connect to the database with.
This is fine for development and testing, as it is a low-friction means
of getting going with switchman, and switchman does its best to support
this (or any other legacy infrastructure) by creating a default shard
with NULL name, and detecting the schema it connected to (based on
the settings in database.yml, and the current database structure).

However, in a production environment, this is potentially dangerous. This
is due to potential confusion by including public in your search path
anyway. For example, say you have a shard named "shard2", and a default
shard named "public". Your search path will likely end up being
"shard2,public". This means that if for some reason shard2 becomes
inaccessible (permission issues most likely), switchman will not be
aware that the tables that it is seeing are actually coming from the
public schema -- the default shard! Your app will just happily go on
corrupting data because two logical shards are being serviced by a
single underlying schema.

Another small tip is to prefer to uniquely name your shards.
Switchman tries to do this automatically by naming a shard with the
ID as part of the name, but you're more than welcome to override this.
The problem is that if you have the same schema name on multiple
database servers (say "myapp"), and later decide to move them around
to rebalance load, you'll have the additional headache of needing to
rename the shards and schemas so that you don't end up with multiple
shards of the same name on the same database server.
