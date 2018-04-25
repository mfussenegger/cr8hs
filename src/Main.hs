{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Control.Concurrent.Async (async, wait)
import           Control.Monad            (unless, replicateM)
import           Data.Aeson               ((.:), (.=))
import qualified Data.Aeson               as A
import qualified Data.Aeson.Types         as A
import qualified Data.ByteString          as B
import qualified Data.ByteString.Lazy     as BL
import           Data.Semigroup           ((<>))
import qualified Data.Text                as T
import           GHC.Generics
import           Network.HTTP.Client      (Manager, newManager)
import qualified Network.HTTP.Client      as NC
import           Network.HTTP.Client.TLS  (tlsManagerSettings)
import           Options.Applicative
import           Pipes
import qualified Pipes.Concurrent         as P
import           System.IO                (isEOF)


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


fromStdin :: Producer B.ByteString IO ()
fromStdin = do
  eof <- lift isEOF
  unless eof $ do
    line <- lift B.getLine
    yield line
    fromStdin


data Query = Query
  { stmt :: T.Text
  , args :: [A.Value]
  , mode :: T.Text }
  deriving (Generic, Show)

instance A.FromJSON Query


parseQuery :: Monad m => Pipe B.ByteString Query m ()
parseQuery = do
  line <- await
  case (A.eitherDecodeStrict line :: Either String Query) of
    Left msg    -> error $ "Invalid query input: " <> msg
    Right query -> do
      yield query
      parseQuery


execQuery :: String -> Manager -> Pipe Query Double IO ()
execQuery url manager = do
  query <- await
  respBody <- lift $ sendQuery query
  case getDuration respBody of
    Nothing -> error $ "duration missing in responseBody: " <> show respBody
    Just duration -> do
      yield duration
      execQuery url manager
  where
    getDuration :: BL.ByteString -> Maybe Double
    getDuration resp = A.decode resp >>= A.parseMaybe (.: "duration")
    sendQuery query = do
      initReq <- NC.parseRequest url
      let
        reqObj = case mode query of
          "single" -> A.object [ "stmt" .= stmt query
                               , "args" .= args query ]
          "bulk"   -> A.object [ "stmt" .= stmt query
                               , "bulk_args" .= args query ]
          m        -> error $ "Invalid query mode" <> show m
        body = NC.RequestBodyLBS (A.encode reqObj)
        req = initReq 
          { NC.method = "POST"
          , NC.requestBody = body
          , NC.requestHeaders = [ ("Content-Type", "application/json") ]
          }
      NC.responseBody <$> NC.httpLbs req manager


main :: IO ()
main = do
  cliArgs <- execParser parser
  manager <- newManager tlsManagerSettings
    { NC.managerConnCount = concurrency cliArgs
    , NC.managerResponseTimeout = NC.responseTimeoutNone }
  let
    url = "http://" <> hosts cliArgs <> "/_sql"
    execQuery' = execQuery url manager
    concurrency' = concurrency cliArgs
    processQueries = for execQuery' (lift . print)
    buffer = P.bounded (concurrency' * 2)
  (output, input, seal) <- P.spawn' buffer
  workers <- replicateM concurrency' $ async $ do
    runEffect $ P.fromInput input >-> parseQuery >-> processQueries
    P.atomically seal
  producer <- async $ do
    runEffect $ fromStdin >-> P.toOutput output
    P.atomically seal
  mapM_ wait (producer:workers)
