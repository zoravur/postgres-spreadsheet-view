async function sendQuery() {
    console.log("HELLO");
    const q = document.getElementById('query').value;
    const res = await fetch('/api/query', {
        method: 'POST',
        body: q
    });
    document.getElementById('result').textContent = JSON.stringify(
        await res.json(),
        null,
        2
    );
}

async function getProvenance() {
  const q = document.getElementById('query').value;
  const res = await fetch('/api/provenance', {
    method: 'POST',
    body: q
  });
  const data = await res.json();
  document.getElementById('result').textContent = JSON.stringify(data, null, 2);
}

document.addEventListener("DOMContentLoaded", () => {
  document.getElementById('run').addEventListener("click", () => {
    sendQuery();
  });
  document.getElementById('provenance').addEventListener("click", () => {
    getProvenance();
  });
});