{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Control.Monad           (unless, join)
import           Data.Aeson              ((.:), (.=))
import qualified Data.Aeson              as A
import qualified Data.Aeson.Types        as A
import qualified Data.ByteString         as B
import qualified Data.ByteString.Lazy    as BL
import           Data.Semigroup          ((<>))
import qualified Data.Text               as T
import           GHC.Generics
import           Network.HTTP.Client     (Manager, newManager)
import qualified Network.HTTP.Client     as NC
import           Network.HTTP.Client.TLS (tlsManagerSettings)
import           Options.Applicative
import           Pipes
import qualified Pipes.Prelude           as P
import           System.IO               (isEOF)


data Args = Args
  { hosts :: String
  , concurrency :: Int }
  deriving (Show)


parser :: ParserInfo Args
parser = parserInfo
  where
    parserInfo = info (argsDef <**> helper)
      ( fullDesc
      <> progDesc "Execute queries received on stdin" )
    argsDef :: Parser Args
    argsDef = Args
      <$> strOption
        ( long "hosts"
        <> metavar "HOSTS"
        <> showDefault
        <> value "localhost:4200" )
      <*> option auto
        ( long "concurrency" 
        <> metavar "CONCURRENCY"
        <> showDefault
        <> value 1 )


fromStdin :: Producer BL.ByteString IO ()
fromStdin = do
  eof <- lift isEOF
  unless eof $ do
    line <- lift $ BL.fromStrict <$> B.getLine
    yield line
    fromStdin


data Query = Query
  { stmt :: T.Text
  , args :: [A.Value] }
  deriving (Generic, Show)

instance A.FromJSON Query


parseQuery :: Monad m => Pipe BL.ByteString (Maybe Query) m ()
parseQuery = do
  line <- await
  yield (A.decode line)
  parseQuery


execQuery :: String -> Manager -> Pipe (Maybe Query) Double IO ()
execQuery host manager = do
  maybeQuery <- await
  case maybeQuery of
    Nothing -> execQuery host manager
    Just query -> do
      respBody <- lift $ sendQuery query
      case getDuration respBody of
        Nothing -> execQuery host manager
        Just duration -> do
          yield duration
          execQuery host manager
  where
    getDuration :: BL.ByteString -> Maybe Double
    getDuration resp = A.decode resp >>= A.parseMaybe (.: "duration")
    sendQuery query = do
      initReq <- NC.parseRequest ("http://" <> host <> "/_sql")
      let
        reqObj = A.object
          [ "stmt" .= stmt query
          , "args" .= args query ]
        body = NC.RequestBodyBS (BL.toStrict $ A.encode reqObj)
        req = initReq 
          { NC.method = "POST"
          , NC.requestBody = body
          , NC.requestHeaders = [ ("Content-Type", "application/json") ]
          }
      NC.responseBody <$> NC.httpLbs req manager


printResult :: Consumer Double IO ()
printResult = do
  result <- await
  lift $ print result


main :: IO ()
main = do
  args <- execParser parser
  manager <- newManager tlsManagerSettings
  let
    queries :: Producer (Maybe Query) IO ()
    queries = fromStdin >-> parseQuery
    results :: Producer Double IO ()
    results = queries >-> execQuery (hosts args) manager
  runEffect $ for results (lift . print)
