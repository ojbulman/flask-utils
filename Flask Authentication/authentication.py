import os
from functools import wraps

import jwt
import config
from flask import Blueprint, flash, jsonify, redirect, render_template, request

bp = Blueprint('authentication', __name__)

def authenticate(username:str=None, password:str=None, token:str=None) -> bool:
    # Insert code here to authenticate users
    jwt_token = ""
    return jwt_token


def authorize(role:str,feature_key:str=None):
    """Decorator Function to ensure API Request Authentication"""
    def wrapper(fn):
        @wraps(fn)
        def decorated_view(*args, **kwargs):
            jwt_token = None
            valid = False
            user_pid = None

            #Check Cookie for auth id & token
            if ("token" in request.cookies):
                jwt_token = request.cookies.get('token')
            
            if (jwt_token is not None):
                try:
                    jwt_data = jwt.decode(jwt_token, os.getenv('platform_secret_key'), algorithms="HS256")
                    user_pid = jwt_data['pid']

                    if role in  jwt_data['apps'][config.APP_TAG]['roles']:
                        valid = True

                        if feature_key:
                            if feature_key in jwt_data['apps'][config.APP_TAG]['feature_keys']:
                                valid = False
                        
                except Exception as e:
                    print(e)
                    valid = False


            if not valid:
                redir = redirect("/auth/login/", code=302)
                redir.set_cookie('token', '', expires=0)
                flash('Login Required')
                return redir

            return fn(user_pid, *args, **kwargs)
        return decorated_view
    return wrapper


@bp.route("/login/", methods=['GET'])
def login_get():
    return render_template("login.html")

@bp.route("/login/", methods=['POST'])
def login_post():
    return render_template("login.html")


@bp.route("/logout/", methods=['GET'])
def logout_get():
    return render_template("login.html")


@bp.route("/pwchange/", methods=['GET'])
def pwchange_get():
    return render_template("pwchange.html")

@bp.route("/pwchange/", methods=['POST'])
def pwchange_post():
    return render_template("pwchange.html")


@bp.route("/data/cookie_consent", methods=['GET'])
def cookie_consent_get():
    res = jsonify({"result":True})
    res.set_cookie("cookie_consent", True, max_age=31536000)
    return res