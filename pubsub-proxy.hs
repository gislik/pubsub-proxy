{-# LANGUAGE OverloadedStrings #-}

module Main where

import Data.Maybe                   (fromMaybe)
import Data.ByteString              (ByteString)
import Data.ByteString.Char8        (unpack)
import Control.Applicative          (liftA)
import Control.Monad                (forever, void)
import Control.Concurrent           (forkIO)
import System.Environment           (getEnvironment)
import System.Log.Logger
import System.ZMQ3

--------------------------------------------------------------------------------
-- zeromq
--------------------------------------------------------------------------------
type Dsn = String

logDsn :: Dsn
logDsn = "inproc://logger"

usingSocket :: SocketType a => Context -> Dsn -> a -> (Socket a -> IO b) -> IO b
usingSocket ctx dsn stype cb = withSocket ctx stype $ \s -> bind s dsn >> cb s

usingCapturer :: Context -> (Socket Pair -> IO a) -> IO a
usingCapturer ctx cb = withSocket ctx Pair $ \s -> connect s logDsn >> cb s

usingLogger :: Context -> (Socket Pair -> IO a) -> IO a
usingLogger ctx cb = withSocket ctx Pair $ \s -> bind s logDsn >> cb s

receiveFirst :: Receiver a => Socket a -> IO ByteString
receiveFirst s = liftA head (receiveMulti s)

--------------------------------------------------------------------------------
-- logger
--------------------------------------------------------------------------------
logger :: Context -> IO ()
logger ctx = usingLogger ctx $ \s -> do
   updateGlobalLogger rootLoggerName (setLevel INFO)
   forever $ receiveFirst s >>= infoM "pubsub-proxy.logger" . unpack 

--------------------------------------------------------------------------------
-- main
--------------------------------------------------------------------------------
main :: IO ()
main = do 
   updateGlobalLogger rootLoggerName (setLevel INFO)
   infoM "pubsub-proxy.main" "Starting program"
   env <- getEnvironment
   let pdsn = fromMaybe "tcp://127.0.0.1:7006" $ lookup "ZMQPUBLISHER"  env
   let sdsn = fromMaybe "tcp://127.0.0.1:7004" $ lookup "ZMQSUBSCRIBER" env
   infoM "pubsub-proxy.main" "Starting logger"
   withContext $ \ctx -> do
      void . forkIO $ logger ctx
      debugM "pubsub-proxy.main" "Configuring sockets"
      usingCapturer  ctx           $ \cap -> 
         usingSocket ctx pdsn XPub $ \pub -> 
         usingSocket ctx sdsn XSub $ \sub -> do
            infoM "pubsub-proxy.main" "PubSub proxy"
            proxy pub sub (Just cap) 
