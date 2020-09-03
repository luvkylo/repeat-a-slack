const padStart = (str, what, len) => {
  let output = `${str}`;
  while (output.length < len) {
    output = `${what}${output}`;
  }
  return output;
};

const padDate = (str) => padStart(str, 0, 2);

const getDateObject = (date) => {
  const year = date.getUTCFullYear();
  const month = date.getUTCMonth() + 1;
  const day = date.getUTCDate();
  const yearStr = `${year}`;
  const monthStr = padStart(month, 0, 2);
  const dayStr = padStart(day, 0, 2);

  return {
    year,
    month,
    day,
    //
    yearStr,
    monthStr,
    dayStr,
    //
    fullStr: `${monthStr}/${dayStr}/${yearStr}`,
  };
};

module.exports = {
  padStart,
  padDate,
  getDateObject,
};
