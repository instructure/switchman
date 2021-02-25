== Upgrading From Switchman 2.0/Rails 6.0 or older ==

With Rails 6.1, Rails has sufficient support for both horizontal and vertical
sharding to implement Switchman's connection management on top of, instead
of significantly replacing a good chunk of Rail's internal code. This has
resulted in some major changes:
 * GuardRail no longer supports a global configuration and modifying it.
   Instead you should use an ERB for your database.yml so that whatever
   you were going to apply is already done there.
 * Shard categories are no more. Instead, you should use a parent class,
   and call `sharded_model` on it to fulfill the same purpose. To get the
   shard category of a class, call its `connection_classes` method (I admit,
   it is oddly named. Talk to Rails). This also means any `activate` methods
   that took symbols now takes the actual class.
 * Separate connection trees can no longer share connections. In particular
   this means that for unsharded models, they will not share a connection
   with the default shard for regular models. In practice, if you're having
   problems with too many connections to the default shard now, you're
   accessing the default shard too much and likely need to improve your
   caching in order to avoid this.
 * In specs, @shard3 is no longer available. @shard1 is now guaranteed to
   share a connection with the default shard.
