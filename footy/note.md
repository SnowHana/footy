# Flask Basics

## 1. Basics

```python
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

```

- Single function works as a single page
- But how do we know which function is a flask webpage and which is not a flask webpage (ie. Normal python function)?
  - Thats where **decorator** kicks in

### @app.route({path}) decorator

{path} declares path to access that web page or a function

#### Passing function parameter

app.route("{\<parameter\>}")
