class Timer {
  constructor(callback, time) {
    this.setTimeout(callback, time);
  }

  setTimeout(callback, time) {
    const self = this;
    if (this.timer) {
      clearTimeout(this.timer);
    }
    this.finished = false;
    this.callback = callback;
    this.time = time;
    this.timer = setTimeout(() => {
      self.finished = true;
      callback();
    }, time);
    this.start = Date.now();
  }

  add(time) {
    if (!this.finished) {
      // add time to time left
      let t = time;
      t = this.time - (Date.now() - this.start) + time;
      this.setTimeout(this.callback, t);
    }
  }
}


// function Timer(callback, time) {
//   this.setTimeout(callback, time);
// }

// // eslint-disable-next-line func-names
// Timer.prototype.setTimeout = function (callback, time) {
//   const self = this;
//   if (this.timer) {
//     clearTimeout(this.timer);
//   }
//   this.finished = false;
//   this.callback = callback;
//   this.time = time;
//   this.timer = setTimeout(() => {
//     self.finished = true;
//     callback();
//   }, time);
//   this.start = Date.now();
// };

// // eslint-disable-next-line func-names
// Timer.prototype.add = function (time) {
//   if (!this.finished) {
//     // add time to time left
//     let t = time;
//     t = this.time - (Date.now() - this.start) + time;
//     this.setTimeout(this.callback, t);
//   }
// };

module.exports = {
  Timer,
};
