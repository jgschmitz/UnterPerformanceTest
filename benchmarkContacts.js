// 5. Sample contacts for benchmark input
db.contacts.aggregate([
  {
    $sample: {
      size: 5000
    }
  },
  {
    $project: {
      _id: 0,
      contact_id: 1,
      customer_id: 1,
      date: 1,
      driver_id: "$trip.driver_id"
    }
  }
])
