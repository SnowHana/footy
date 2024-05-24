from flask import Flask, render_template, request
from scraping import Squad

app = Flask(__name__)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/graph", methods=["POST"])
def graph():
    feature = request.form["feature"]
    squad = Squad()
    plot = squad.avg_feature_graph(
        feature
    )  # Assuming avg_feature_graph returns plot object
    return render_template("graph.html", plot=plot)  # Pass plot object to template


if __name__ == "__main__":
    app.run(debug=True)
