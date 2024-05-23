from flask import Flask, redirect, url_for, render_template

app = Flask(__name__)


# Rendering html
# Display name : Function parameter on html?
@app.route("/<name>")
def home(name):
    # return render_template("index.html", content=name, r=521)
    return render_template("index.html", content=["lukaku", "sam", "joe"])


if __name__ == "__main__":
    app.run()
