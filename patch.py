import re

with open("benches/cpp_like_bench.rs", "r") as f:
    text = f.read()

pattern = r"    for run in 0\.\.10 \{\n        let engine = Arc::new\(Exchange::new\(num_threads\)\);\n\s+let start = Instant::now\(\);"

replacement = r"""    let engine = Arc::new(Exchange::new(num_threads));
    
    for run in 0..10 {
        let start = Instant::now();"""

text = re.sub(pattern, replacement, text)

with open("benches/cpp_like_bench.rs", "w") as f:
    f.write(text)
