var express = require("express");
var app = express();


app.listen(3000, () => {
    console.log("Server running on port 3000");
});

app.get("/api", (req, res, next) => {
    if (req.query.timeFrame == null) {
        res.json({
            "id": req.query.id,
        });
    }
    else {
        res.json({
            "id": req.query.id,
            "timeFrame": req.query.timeFrame,
        });
    }
});

