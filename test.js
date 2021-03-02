const send = require('gmail-send')({
  user: 'alvin@frequency.com',
  pass: '19971124s030111',
  to: 'analyze-jukin@frequency.com',
  subject: 'test',
});

send({ text: 'test' }).then(({ result, full }) => console.log(result))
  .catch((error) => console.error('ERROR', error));
