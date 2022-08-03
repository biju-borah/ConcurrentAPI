const bodyParser = require("body-parser");
var express = require("express");
var app = express();

app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

app.post("/", (req, res, next) => {
    console.log(req.body)
    res.json(req.body)
})
app.get("/", (req, res, next) => {
    if (req.query.timeFrame == null) {
        res.json(
            {
                data: {
                    a: 1,
                    b: 2,
                    c: 3,
                    d: 4,
                    e: 5,
                }
            }
        );
    }
    else {
        res.json(
            {
                data: {
                    a: 1,
                    b: 2,
                    c: 3,
                    d: 4,
                    e: 5,
                }
            }
        );
    }
});

app.listen(process.env.PORT, () => {
    console.log("Server running ");
});