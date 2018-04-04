#Coding: utf-8 -*-
"""
    MiniTwit
    ~~~~~~~~

    A microblogging application written with Flask and MYSQL.

    :copyright: (c) 2015 by Armin Ronacher.
    :license: BSD, see LICENSE for more details.
"""

import os
import time
import MySQLdb
from MySQLdb.cursors import DictCursor
from hashlib import md5
from datetime import datetime
from flask import Flask, request, session, url_for, redirect, \
     render_template, abort, g, flash, _app_ctx_stack
from werkzeug import check_password_hash, generate_password_hash


PER_PAGE = 30
DEBUG = True
SECRET_KEY = os.getenv('SECRET_KEY') or 'hard to guess string'

# create our little application :)
app = Flask('minitwit')
app.config.from_object(__name__)
app.config.from_envvar('MINITWIT_SETTINGS', silent=True)


def get_db():
    top = _app_ctx_stack.top
    if not hasattr(top, 'db'):
        top.db = MySQLdb.connect(host="minitwitflask-cluster-1.cluster-cqtoaygh4yjh.us-west-2.rds.amazonaws.com", user="Minitwituser", passwd="foshure1", db="MinitwitDB", port=3306, cursorclass=DictCursor)
    return top.db

def get_connection():
    top = _app_ctx_stack.top
    if not hasattr(top, 'db'):
        top.db = MySQLdb.connect(host="minitwitflask-cluster-1.cluster-cqtoaygh4yjh.us-west-2.rds.amazonaws.com", user="Minitwituser", passwd="foshure1", db="MinitwitDB", port=3306)
    return top.db

@app.teardown_appcontext
def close_database(exception):
    """Closes the database again at the end of the request."""
    top = _app_ctx_stack.top
    if hasattr(top, 'db'):
        top.db.close()

def init_db():
    """Initializes the database."""
    #db = get_db().cursor()
    conn = get_connection()
    db = get_db().cursor()
    with app.open_resource('schema.sql', mode='r') as f:
        db.execute(f.read())
    conn.commit()


@app.cli.command('initdb')
def initdb_command():
    """Creates the database tables."""
    init_db()
    print('Initialized the database.')


def query_db(query, args=(), one=False):
    """Queries the database and returns a list of dictionaries."""
    cur = get_db().cursor()
    n = cur.execute(query, args)
    rv = cur.fetchall()
    return (rv[0] if rv else None) if one else rv

def get_user_id(username):
    """Convenience method to look up the id for a username."""
    rv = query_db('select user_id from user where username = %s',
                  [username], one=True)
    return rv['user_id'] if rv else None


def format_datetime(timestamp):
    """Format a timestamp for display."""
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d @ %H:%M')


def gravatar_url(email, size=80):
    """Return the gravatar image for the given email address."""
    return 'https://secure.gravatar.com/avatar/%s?d=identicon&s=%d' % \
        (md5(email.strip().lower().encode('utf-8')).hexdigest(), size)


@app.before_request
def before_request():
    g.user = None
    if 'user_id' in session:
        g.user = query_db('select * from user where user_id = %s',
                          [session['user_id']], one=True)


@app.route('/')
def timeline():
    """Shows a users timeline or if no user is logged in it will
    redirect to the public timeline.  This timeline shows the user's
    messages as well as all the messages of followed users.
    """
    if not g.user:
        return redirect(url_for('public_timeline'))
    return render_template('timeline.html', messages=query_db('''
        select message.*, user.* from message, user
        where message.author_id = user.user_id and (
            user.user_id = %s or
            user.user_id in (select whom_id from follower
                                    where who_id = %s))
        order by message.pub_date desc limit %s''',
        [session['user_id'], session['user_id'], PER_PAGE]))


@app.route('/public')
def public_timeline():
    """Displays the latest messages of all users."""
    return render_template('timeline.html', messages=query_db('''
        select message.*, user.* from message, user
        where message.author_id = user.user_id
        order by message.pub_date desc limit %s''', [PER_PAGE]))


@app.route('/<username>')
def user_timeline(username):
    """Display's a users tweets."""
    profile_user = query_db('select * from user where username = %s',
                            [username], one=True)
    if profile_user is None:
        abort(404)
    followed = False
    if g.user:
        followed = query_db('''select 1 from follower where
            follower.who_id = %s and follower.whom_id = %s''',
            [session['user_id'], profile_user['user_id']],
            one=True) is not None
    return render_template('timeline.html', messages=query_db('''
            select message.*, user.* from message, user where
            user.user_id = message.author_id and user.user_id = %s
            order by message.pub_date desc limit %s''',
            [profile_user['user_id'], PER_PAGE]), followed=followed,
            profile_user=profile_user)


@app.route('/<username>/follow')
def follow_user(username):
    """Adds the current user as follower of the given user."""
    if not g.user:
        abort(401)
    whom_id = get_user_id(username)
    if whom_id is None:
        abort(404)
    conn = get_connection()    
    db = get_db().cursor()
    db.execute('insert into follower (who_id, whom_id) values (%s, %s)',
              [session['user_id'], whom_id])
    conn.commit()
    flash('You are now following "%s"' % username)
    return redirect(url_for('user_timeline', username=username))


@app.route('/<username>/unfollow')
def unfollow_user(username):
    """Removes the current user as follower of the given user."""
    if not g.user:
        abort(401)
    whom_id = get_user_id(username)
    if whom_id is None:
        abort(404)
    db = get_db()
    conn = get_connection()    
    #db = conn
    db.execute('delete from follower where who_id=%s and whom_id=%s',
              [session['user_id'], whom_id])
    conn.commit()
    flash('You are no longer following "%s"' % username)
    return redirect(url_for('user_timeline', username=username))


@app.route('/add_message', methods=['POST'])
def add_message():
    """Registers a new message for the user."""
    if 'user_id' not in session:
        abort(401)
    if request.form['text']:
	conn = get_connection()       
 	db = get_db()#.cursor()
        db.execute('''insert into message (author_id, text, pub_date)
          values (%s, %s, %s)''', (session['user_id'], request.form['text'],
                                int(time.time())))
	conn.commit()
        flash('Your message was recorded')
    return redirect(url_for('timeline'))

@app.route('/del_message/<int:id>')
def del_message(id):
    """Registers a new message for the user."""
    if 'user_id' not in session:
        abort(401)
    db = get_db()#.cursor()
    db.execute('''delete from message where author_id=%s and message_id=%s''',
               [session['user_id'], id])
    flash('Your message was deleted')
    return redirect(url_for('timeline'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    """Logs the user in."""
    if g.user:
        return redirect(url_for('timeline'))
    error = None
    if request.method == 'POST':
        user = query_db('''select * from user where
            username = %s''', [request.form['username']], one=True)
        if user is None:
            error = 'Invalid username'
        elif not check_password_hash(user['pw_hash'],
                                     request.form['password']):
            error = 'Invalid password'
        else:
            flash('You were logged in')
            session['user_id'] = user['user_id']
            return redirect(url_for('timeline'))
    return render_template('login.html', error=error)


@app.route('/register', methods=['GET', 'POST'])
def register():
    """Registers the user."""
    if g.user:
        return redirect(url_for('timeline'))
    error = None
    if request.method == 'POST':
        if not request.form['username']:
            error = 'You have to enter a username'
        elif not request.form['email'] or \
                '@' not in request.form['email']:
            error = 'You have to enter a valid email address'
        elif not request.form['password']:
            error = 'You have to enter a password'
        elif request.form['password'] != request.form['password2']:
            error = 'The two passwords do not match'
        elif get_user_id(request.form['username']) is not None:
            error = 'The username is already taken'
        else:
	    conn= get_db()
	    db = conn.cursor()
            db.execute('''insert into user (
              username, email, pw_hash) values (%s, %s, %s)''',
              [request.form['username'], request.form['email'],
               generate_password_hash(request.form['password'])])
	    conn.commit()
            flash('You were successfully registered and can login now')
            return redirect(url_for('login'))
    return render_template('register.html', error=error)


@app.route('/logout')
def logout():
    """Logs the user out."""
    flash('You were logged out')
    session.pop('user_id', None)
    return redirect(url_for('public_timeline'))


# add some filters to jinja
app.jinja_env.filters['datetimeformat'] = format_datetime
app.jinja_env.filters['gravatar'] = gravatar_url

if __name__ == '__main__':
    app.run()

