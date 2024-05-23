from flask import Flask, redirect, url_for

# redirect, url_for : For redirection

app = Flask(__name__)


@app.route("/")
def home():
    return "Hello! this is the main page. <h1>HELLO<h1>"


# "/<{parameter}>" : Value between < and > is delivered as a function parameter.
@app.route("/<name>")
def user(name):
    return f"Hello {name}"


@app.route("/admin")
def admin():
    # Redirection
    # Put name of function we're redirecting to
    # 1. Function with no param
    # return redirect(url_for("home"))

    return redirect(url_for("user", name="Admin!"))


if __name__ == "__main__":
    app.run()
