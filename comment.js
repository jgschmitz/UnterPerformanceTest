// 3. Add comment to a contact
db.contacts.updateOne(
  {
    contact_id: "<contact_id>"
  },
  {
    $push: {
      comments: {
        ts: new Date(),
        text: "qps benchmark comment"
      }
    }
  },
  {
    hint: "contact_id_unique"
  }
)
