from flask import Flask, redirect, url_for, render_template

app = Flask(__name__)


# Rendering html
# Display name : Function parameter on html?
@app.route("/")
def home():
    # return render_template("index.html", content=name, r=521)
    return render_template("index.html", content="Roh")


@app.route("/test")
def test():
    # return render_template("index.html", content=name, r=521)
    return render_template("new.html")


if __name__ == "__main__":
    app.run(debug=True)
