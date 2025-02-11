from flask import Flask
from pattern.TestShape import find_bottom_line
app = Flask(__name__)

@app.route('/find_bottom_line')
def find():
    value = find_bottom_line()
    return value

if __name__ == '__main__':
    app.run(debug=True)
