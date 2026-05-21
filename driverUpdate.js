// 4. Driver averages / top driver summaries
db.driver_summary.find(
  {},
  {
    _id: 0
  }
)
.sort({ rides: -1 })
.limit(100)
.hint("rides_desc")
