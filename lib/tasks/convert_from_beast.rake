desc %q{
Convert an SQL dump of Beast data to Thredded, assuming a PostgreSQL source.
The 'psql' command must be available. Run immediately after creating and
migrating a CLEAN Thredded database and pass the SQL dump filename via:

  bundle exec convert_from_beast[path/to/beast_sql_dump.sql]

Table renames occur, Beast data is loaded from an SQL dump via 'psql' into
tables renamed all with a "beast_" prefix and Thredded table names put back.
Data is then converted. This may take an extremely long time, even for only
moderate sized forums. Users, forums, topics, posts and active user
monitorships are converted; some dedupe is done where expected, else things
will fail.

IMPORTANT: User passwords cannot be converted. The encryption mechanisms
are different and one-way, so we cannot decrypt and 'recrypt'. Instead,
user passwords are all converted to "cannot-convert" unless you take steps
to change that, e.g. randomise, e-mail users with the random value and
insist on an immediate change on next login. There's no good answer here.

Beast doesn't have groups of forums, so a default messageboard group with
name "Main" is created to hold all of the forums, their topics and the
posts within those topics.

Content bodies in Beast are probably in Textile, but Thredded out of the
box uses Kramdown. Either adjust this code to add a converter, take the
pre-rendered HTML body from Beast and lose the user-editable version
during migration instead (around line 297 at the time of writing, assign
to Thredded's "content" attribute the Beast post's "body_html" attribute
value instead of "body"), or patch Thredded to understand Textile.

To avoid PostgreSQL grinding to a halt, no wrapping transaction is used.
Changes are permanent. If conversion fails, you'll need to find out why,
fix this Rake task, run "rake db:drop db:create db:migrate" to clean out
the half-converted data, then re-run the conversion.

Afterwards the "beast_..." tables are left in place in case you want to do
any additional checks or conversions. Manually drop these at the database
console ("rails dbconsole") when you're finished with them.

Based on and with thanks to:

  https://github.com/thredded/thredded/wiki/Migrate-from-Forem
}
  
task :convert_from_beast, [ :beast_sql_path ] => [ :environment ] do | t, args |

  now        = Time.zone.now
  connection = ActiveRecord::Base.connection
  old_logger = ActiveRecord::Base.logger

  ActiveRecord::Base.logger = nil

  beast_sql_path = args[ :beast_sql_path ]
  raise 'Usage: rake convert_from_beast[path/to/beast_sql_dump.sql]' unless beast_sql_path.present?

  # Have to skip a bunch of callbacks to avoid e.g. e-mails being sent - if
  # you were migrating in Production, this would be catastrophic!
  #
  skip_callbacks = [
    [Thredded::Post, :commit, :after, :update_parent_last_user_and_time_from_last_post],
    [Thredded::PrivatePost, :commit, :after, :update_parent_last_user_and_timestamp],
    [Thredded::Post, :commit, :after, :auto_follow_and_notify],
    [Thredded::PrivatePost, :commit, :after, :notify_users],
    [User, :update, :after, :send_password_change_notification],
  ]

  skip_callbacks.each { |(klass, *args)| klass.skip_callback(*args) }

  # Try to clear out dirty data from a prior run, though other tables may have
  # been added to via callbacks; really the user should drop and recreate the
  # Thredded database each time, but they might forget.
  #
  [Thredded::Messageboard, Thredded::MessageboardGroup, Thredded::Topic, Thredded::Post,
   Thredded::UserTopicFollow, Thredded::UserDetail, User].each do |klass|
    puts "Deleting #{klass.name}..."
    klass.delete_all
  end

  # Move Thredded 'users' table and index data out of the way of the Beast
  # 'users' table, temporarily.
  #
  connection.execute( 'ALTER TABLE users RENAME TO thredded_users;' );
  connection.execute( 'ALTER TABLE users_id_seq RENAME TO thredded_users_id_seq;' );

  # Load the Beast data.
  #
  result = `psql -d #{ connection.current_database } -f #{ beast_sql_path }`
  
  # Drop stuff we don't want and rename the rest with the "beast_" prefix.
  #
  connection.execute( 'DROP TABLE schema_info;' );
  connection.execute( 'DROP TABLE sessions;' );
  connection.execute( 'DROP TABLE logged_exceptions;' );
  connection.execute( 'DROP TABLE moderatorships;' );

  connection.execute( 'ALTER TABLE topics RENAME TO beast_topics;' );
  connection.execute( 'ALTER TABLE topics_id_seq RENAME TO beast_topics_id_seq;' );
  connection.execute( 'ALTER TABLE users RENAME TO beast_users;' );
  connection.execute( 'ALTER TABLE users_id_seq RENAME TO beast_users_id_seq;' );
  connection.execute( 'ALTER TABLE forums RENAME TO beast_forums;' );
  connection.execute( 'ALTER TABLE forums_id_seq RENAME TO beast_forums_id_seq;' );
  connection.execute( 'ALTER TABLE monitorships RENAME TO beast_monitorships;' );
  connection.execute( 'ALTER TABLE monitorships_id_seq RENAME TO beast_monitorships_id_seq;' );
  connection.execute( 'ALTER TABLE posts RENAME TO beast_posts;' );
  connection.execute( 'ALTER TABLE posts_id_seq RENAME TO beast_posts_id_seq;' );

  # Put the Threded 'users' table back to its correct name now that the Beast
  # table is called "beast_users".
  #
  connection.execute( 'ALTER TABLE thredded_users RENAME TO users;' );
  connection.execute( 'ALTER TABLE thredded_users_id_seq RENAME TO users_id_seq;' );

  # Now for the main conversion routine.

  beast_data = %i(
      users
      posts
      monitorships
      topics
    ).inject({}) { |h, k|
    h.update k => connection.select_all("SELECT * FROM beast_#{k} ORDER BY id")
  }

  %i(forums).each { |t|
    beast_data[t] = connection.select_all("SELECT * FROM beast_#{t} ORDER BY position")
  }

  puts 'Creating (Devise) Users...'

  user_count = 0
  user_total = beast_data[:users].count

  beast_data[:users].each do | user |
    email = user['email'].blank? ? "hub_record_removed_#{ user_count }@example.com" : user['email']
    email.gsub!("\s", '')

    hash = {
      id:                    user['id'],
      email:                 email,
      password:              'cannot-convert',
      password_confirmation: 'cannot-convert',
      last_sign_in_at:       user['last_login_at'],
      admin:                 user['admin'],
      display_name:          user['display_name'],
      created_at:            user['created_at']
    }

    other_user = User.where( 'display_name ILIKE ?', user['display_name'].downcase() ).first
    other_user = User.where( 'email ILIKE ?', email.downcase() ).first if other_user.nil?
    
    unless other_user.nil?
      dupe_email = "hub_record_duplication_#{ user_count }@example.com"
      dupe_name  = "#{ user['display_name'] } #{ user_count }"

      if user[ 'id' ].to_i < other_user.id.to_i
        kept_id                 = other_user.id
        replaced_id             = user[ 'id' ]
        hash[ 'email'        ]  = dupe_email
        hash[ 'display_name' ]  = dupe_name
      else
        kept_id                 = user[ 'id' ]
        replaced_id             = other_user.id
        other_user.email        = dupe_email
        other_user.display_name = dupe_name
        other_user.save!
      end

      puts "WARNING: Duplicate data in ID #{replaced_id} removed in favour of ID #{kept_id}"
    end

    begin
      User.create!( hash )
    rescue => e
      PP.pp(user)
      raise e.message
    end

    user_count += 1
    puts "#{ user_count } of #{ user_total }" if user_count % 50 == 0
  end

  puts "Created #{ user_count } Users"
  puts 'Creating UserDetails...'

  user_details = beast_data[:posts].group_by { |p| p['user_id'] }.map do |user_id, user_posts|
    latest_activity = user_posts.max_by { |p| p['created_at'] }['created_at']
    Thredded::UserDetail.create!( {
      id:                 user_id,
      user_id:            user_id,
      latest_activity_at: latest_activity,
      created_at:         latest_activity,
      updated_at:         latest_activity,
      moderation_state:   :approved
    } )
  end

  puts "Created #{user_details.length} UserDetails"
  puts 'Creating default "Main" Messageboard Groups'

  Thredded::MessageboardGroup.create!(
    name: 'Main',
    created_at: now,
    updated_at: now
  )

  puts 'Created "Main" MessageboardGroup'
  puts 'Copying Messageboards...'

  group_id = Thredded::MessageboardGroup.last.id
  boards   = {}

  beast_data[:forums].each { |f|
    forum_id = f['id']
    boards[forum_id] = Thredded::Messageboard.create!( {
      id:                    forum_id,
      name:                  f['name'],
      description:           f['description'],
      slug:                  forum_id, # (sic.)
      messageboard_group_id: group_id,
      created_at:            now,
      updated_at:            now
    } )
  }

  puts "Created #{boards.size} Messageboards"
  puts 'Copying Topics...'

  topic_total          = beast_data[:topics].count
  topic_count          = 0
  topics               = {}
  beast_posts_by_topic = beast_data[:posts].group_by { |p| p['topic_id'] }

  beast_data[:topics].each { |t|
    topic_count += 1
    puts "#{ topic_count } of #{ topic_total }" if topic_count % 500 == 0

    topic_id = t['id']
    last_post = beast_posts_by_topic[topic_id].max_by { |p| p['created_at'] }
    topics[topic_id] = Thredded::Topic.create!( {
      id:               topic_id,
      messageboard_id:  boards[t['forum_id']].id,
      user_id:          t['user_id'],
      title:            t['title'],
      slug:             topic_id,
      sticky:           (t['sticky'] == 1 ? true : false),
      locked:           t['locked'],
      created_at:       t['created_at'],
      updated_at:       last_post['created_at'],
      last_post_at:     last_post['created_at'],
      last_user_id:     last_post['user_id'],
      moderation_state: :approved
    } )
  }

  puts "Created #{topics.size} Topics"
  puts 'Creating Beast Subscriptions to UserTopicFollows...'

  subs_count = 0

  beast_data[:monitorships].each do |sub|
    next unless sub['active'] == true
    topic = topics[sub['topic_id']]
    next unless topic

    begin
      subs_count += 1
      Thredded::UserTopicFollow.create!( {
          id:         sub['id'],
          user_id:    sub['user_id'],
          topic_id:   topic.id,
          reason:     :manual,
          created_at: now
      } )
    rescue ActiveRecord::RecordNotUnique
      # Ignore this exception; Beast data really does contain duplicates!
    end
  end

  puts "Created #{subs_count} UserTopicFollows..."
  puts 'Copying Posts...'

  post_total = beast_data[:posts].count
  post_count = 0
  posts = {}
  beast_data[:posts].each { |p|
    post_count += 1
    puts "#{ post_count } of #{ post_total }" if post_count % 500 == 0

    post_id = p['id']
    topic = topics[p['topic_id']]
    posts[post_id] = Thredded::Post.create!( {
      id:               post_id,
      user_id:          p['user_id'],
      messageboard_id:  topic.messageboard_id,
      postable_id:      topic.id,
      created_at:       p['created_at'],
      updated_at:       p['updated_at'],
      content:          p['body'],
      moderation_state: :approved
    } )
  }

  puts "Created #{posts.size} Posts"
  puts 'Updating counters'

  boards.each { |_k, v| Thredded::Messageboard.reset_counters(v.id, :topics, :posts) }
  topics.each { |_k, v| Thredded::Topic.reset_counters(v.id, :posts) }
  user_details.each { |v| Thredded::UserDetail.reset_counters(v.id, :topics, :posts) }

  puts
  puts '=' * 80
  puts 'Success!'
  puts '=' * 80
  puts

end
