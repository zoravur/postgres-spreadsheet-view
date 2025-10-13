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

document.addEventListener("DOMContentLoaded", () => {
    document.getElementById('run').addEventListener("click", () => {console.log("event"); sendQuery();} );
});