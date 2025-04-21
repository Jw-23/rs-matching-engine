import re

with open("benches/cpp_like_bench.rs", "r") as f:
    content = f.read()

# Move engine creation outside the loop
pattern = r"    for run in 0\.\.10 \{\n        let engine = Arc::new\(Exchange::new\(num_threads\)\);"
replacement = r"""    let engine = Arc::new(Exchange::new(num_threads));
    
    for run in 0..10 {
"""
content = re.sub(pattern, replacement, content)

# But wait, stop_collector and collector are created PER RUN right now!
# Can we just recreate the trade_rx per run? No engine.take_trade_rx() moves it.
# Let's move the collector outer too.

with open("benches/cpp_like_bench.rs", "w") as f:
    f.write(content)
