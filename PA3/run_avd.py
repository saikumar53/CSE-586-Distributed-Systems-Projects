#!/usr/bin/env python

import sys, os, time, platform

def main():
  if len(sys.argv) != 2:
    print "Please enter the number of AVDs you want to launch"
    print "Usage: " + sys.argv[0] + " <number of AVDs>"
    return

  head = "emulator"
  tail = "&"
  kvm = ""
  if platform.system() == "Windows":
    print "Windows is not supported."
    exit(1)
  elif platform.system() == "Linux":
    head = os.popen("which emulator").read().strip("\n")
    kvm = " -qemu -enable-kvm"
  for i in range(int(sys.argv[1])):
    n = str(i)
    port = str(5554 + i*2)
    cmd = head + " -port " + port + " -avd avd" + n + kvm + tail
    cmd = "cd \"$(dirname \"$(which emulator)\")\" && " + cmd
    print cmd
    os.system(cmd)
    time.sleep(2)

if __name__ == "__main__":
  main()
