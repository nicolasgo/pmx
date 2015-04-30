import redis
import traceback
import hashlib


#
# MD5 for a specific key. Returns a subset of the generated digest
# 
def xx(key, keylen):
  m = hashlib.md5()
  m.update(key)
  return m.hexdigest()[:keylen]

#
# functions to hash user agents, ip addresses and msisdns
# 
def xx_ua(useragent, keylen=32):
  ua = useragent.encode('ascii','ignore')
  return xx(ua, keylen)


def xx_msisdn(msisdn, keylen=16):
  ms = msisdn.encode('ascii','ignore')
  return xx(ms, keylen)


def xx_ip(ip_address, keylen=12):
  ip = ip_address.encode('ascii','ignore')
  return xx(ip, keylen)


#
# State patern for a Redis connection
#
class RedisState(object):

  def __init__(self):
    self.xx_ip_saved = {}
    self.xx_saved = {}
    self.xx_rsaved = {}
    self.xx_ua_saved = {}
 
  def init():
    pass

  def xx_ua(self, useragent, keylen=32):
    added = False
    ua = useragent.encode('ascii','ignore')
    if ua not in self.xx_ua_saved:
      dg = xx_ua(useragent, keylen)
      self.xx_ua_saved[ua] = dg
      added = True
    else:
      dg = self.xx_ua_saved[ua]
    return (ua, dg, added)


  def xx_msisdn(self, msisdn, keylen=16):
    added = False
    ms = msisdn.encode('ascii','ignore')
    if ms not in self.xx_saved:
      dg = xx_msisdn(ms, keylen)
      self.xx_saved[ms] = dg
      self.xx_rsaved[dg] = ms
      added = True
    else:
      dg = self.xx_saved[ms]
    return (ms, dg, added)


  def xx_ip(self, ip_address, keylen=12):
    added = False
    ip = ip_address.encode('ascii','ignore')
    if ip not in self.xx_ip_saved:
      dg = xx_ip(ip, keylen)
      self.xx_ip_saved[ip] = dg
      added = True
    else:
      dg = self.xx_ip_saved[ip]
    return (ip, dg, added)

  def uas(dct, rdct):
    self.xx_saved.update(dct)
    self.xx_rsaved.update(rdct)

  def uas(dct):
    self.xx_ua_saved.update(dct)

  def ips(dct):
    self.xx_ip_saved.update(dct)

 
class Redis(RedisState):

  def __init__(self, proxy):
    super(Redis, self).__init__()
    self.proxy = proxy

  def init(self):
    super(Redis, self).ips(self.redis_server.hgetall('pmx:ip'))
    super(Redis, self).uas(self.redis_server.hgetall('pmx:msisdn'), self.redis_server.hgetall('pmx:rmsisdn'))
    super(Redis, self).uas(self.redis_server.hgetall('pmx:ua'))
 
  def xx_ua(self, useragent, keylen=32):
    (ua, dg, added) = super(Redis, self).xx_ua(useragent, keylen)
    if added:
      self.redis_server.hset('pmx:ua', ua, dg)
    return dg


  def xx_msisdn(self, msisdn, keylen=16):
    (ms, dg, added) = super(Redis, self).xx_msisdn(msisdn, keylen)
    if added:
      self.redis_server.hset('pmx:msisdn', ms, dg)
      self.redis_server.hset('pmx:rmsisdn', dg, ms)
    return dg


  def xx_ip(self, ip_address, keylen=12):
    (ip, dg, added) = super(Redis, self).xx_ip(ip_address, keylen)
    if added:
      self.redis_server.hset('pmx:ip', ip, dg)
    return dg

  def toggle(self):
    self.proxy.state = self.proxy.redis_down
  
 

class RedisDown(RedisState):

  def __init__(self, proxy):
    self.proxy = proxy
    super(RedisDown, self).__init__()

  def init(self):
    pass

  def xx_ua(self, useragent, keylen=32):
    (ua, dg, added) = super(RedisDown, self).xx_ua(useragent, keylen)
    return dg

  def xx_msisdn(self, msisdn, keylen=16):
    (ms, dg, added) = super(RedisDown, self).xx_msisdn(msisdn, keylen)
    return dg

  def xx_ip(self, ip_address, keylen=12):
    (ip, dg, added) = super(RedisDown, self).xx_ip(ip_address, keylen)
    return dg
                  
  def toggle(self):
    self.proxy.state = self.proxy.redis



class RedisProxy():

  def __init__(self):
    # Regular Redis class
    self.redis = Redis(self)
    # Redis is down, take our dummy interface
    self.redis_down = RedisDown(self)
    # Start our state with a functional Redis
    self.state = self.redis

  def init(self):
    self.state.init()

  def toggle(self):
    self.state.toggle()

  def xx_ua(self, useragent, keylen=32):
    return self.state.xx_ua(useragent, keylen)

  def xx_msisdn(self, msisdn, keylen=16):
    return self.state.xx_msisdn(msisdn, keylen)

  def xx_ip(self, ip_address, keylen=12):
    return self.state.xx_ip(ip_address, keylen)


if __name__ == "__main__":
  proxy = RedisProxy();
  try:
    proxy.init()
  except:
    proxy.toggle()
    print "Cannot initialize Redis, toggling"

  print proxy.xx_ip("127.0.0.1")
  print proxy.xx_msisdn("1-800-866-8342")
  print proxy.xx_ua("Mozilla/5.0 (Linux; U; Android 4.0.3; ko-kr; LG-L160L Build/IML74K) AppleWebkit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30")

