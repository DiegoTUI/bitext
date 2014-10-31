import sys

class Trace(object):
  "A tracing class"

  debugmode = False
  quietmode = False
  showlinesmode = False

  prefix = None

  @classmethod
  def debug(cls, message):
    "Show a debug message"
    if not Trace.debugmode or Trace.quietmode:
      return
    Trace.show(message, sys.stdout)

  @classmethod
  def message(cls, message):
    "Show a trace message"
    if Trace.quietmode:
      return
    if Trace.prefix and Trace.showlinesmode:
      message = Trace.prefix + message
    Trace.show(message, sys.stdout)

  @classmethod
  def error(cls, message):
    "Show an error message"
    message = '* ' + message
    if Trace.prefix and Trace.showlinesmode:
      message = Trace.prefix + message
    Trace.show(message, sys.stderr)

  @classmethod
  def fatal(cls, message):
    "Show an error message and terminate"
    Trace.error('FATAL: ' + message)
    exit(-1)

  @classmethod
  def show(cls, message, channel):
    "Show a message out of a channel"
    if sys.version_info < (3,0):
      message = message.encode('utf-8')
    channel.write(message + '\n')