# AVITYPE

A bird song typing game in the style of the classic asteroid-typing shooters
(ZType et al.) — but instead of shooting asteroids, you're flying through a
landscape at night identifying birds.

## How it plays

- Birds appear as faded dark silhouettes in the ecosystem they belong to, at
  roughly the right height (ovenbirds on the ground, tanagers in the canopy,
  kestrels hovering, ospreys way up).
- Each bird sings a stylized, procedurally synthesized song motif with the
  real species' rhythm and pitch contour (chickadee's fee-bee, cardinal's
  rising slurs, kingfisher's rattle, bittern's low pump...).
- Type a bird's name: the first letter locks on, finishing the name reveals
  the bird as glowing line art and replays its song.
- A wrong keystroke scares the bird off (revealed, but fleeing, and your
  combo resets). A bird that escapes off-screen unnamed costs a life.
- Four flyways (forest, wetland, grassland, coast) plus a Migration mode that
  plays through all four. Three difficulties control simultaneous birds,
  scroll speed, and flock size.

## Running

It's a single self-contained `index.html` — no build, no dependencies, no
network. Open it in a browser or serve the directory statically:

    python -m http.server -d games/avitype

Works on touch devices too (tapping the screen summons the keyboard).
