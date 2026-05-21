db.contacts.createIndex(
  { customer_id: 1, date: 1 },
  { name: "customer_date" }
)

db.contacts.createIndex(
  { "trip.driver_id": 1, date: 1 },
  { name: "driver_date" }
)

db.contacts.createIndex(
  { contact_id: 1 },
  { name: "contact_id_unique", unique: true }
)

db.driver_summary.createIndex(
  { rides: -1 },
  { name: "rides_desc" }
)
