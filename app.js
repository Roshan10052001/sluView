fetch("data.json")
  .then(response => response.json())
  .then(reviews => {
    const reviewsSection = document.getElementById("reviews");

    reviews.forEach(r => {
      const card = document.createElement("div");
      card.classList.add("review-card");

      card.innerHTML = `
        <h4>${r.name} · ⭐ ${r.rating} · ${r.date}</h4>
        <p>${r.review}</p>
      `;

      reviewsSection.appendChild(card);
    });
  })
  .catch(error => console.error("Error loading reviews:", error));
