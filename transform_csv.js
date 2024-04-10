
 function process(line) {
  const values = line.split(',');

  // Create new obj and set each field according to destination's schema
  const obj = {
    rank : values[0],
    name : values[1],
    country : values[2]
  };

  return JSON.stringify(obj);
}