// 1. Customer contacts / Rider lookup by customer + month
db.contacts.find(
  {
    customer_id: "<customer_id>",
    date: {
      $gte: "2026-05-01",
      $lt: "2026-06-01"
    }
  },
  {
    _id: 0,
    contact_id: 1,
    customer_id: 1,
    date: 1,
    "trip.driver_id": 1,
    "trip.distance": 1,
    "trip.duration": 1
  }
).hint("customer_date")
