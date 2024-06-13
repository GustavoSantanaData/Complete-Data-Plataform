import React, {useState, useEffect} from 'react'
import api from './api'

const App = () => {
  const [connections, setConnections] = useState([]);
  const [formData, setFormData] = useState({
    nome: '',
    string: '',
    usuario: '',
    senha: ''
  });

  const fetchConnections = async () => {
    const response = await api.get('/connections/');
    setConnections(response.data)
  };

  useEffect(() => {
    fetchConnections();
  }, []);

  const handleInputChange = (event) => {
    const value = event.target.type === 'checkbox' ? event.target.checked : event.target.value;
    setFormData({
      ...formData,
      [event.target.name]: value,
    });
  }

  const handleFormSubmit = async (event) => {
    event.preventDefault();
    await api.post('/connections/', formData);
    fetchConnections();
    setFormData({
      nome: '',
      string: '',
      usuario: '',
      senha: ''
    });
  };

  return (
    <div>
      <nav className='navbar navbar-dark bg-primary'>
        <div className='container-fluid'>
          <a className='navbar-brand' href='/'>
            Frodo
          </a>
        </div>
      </nav>

    <div className='container'>
      <form onSubmit={handleFormSubmit}>
        <div className='mb-3 mt-3'>
          <label htmlFor='nome' className='form-label'>
            Connection Name
          </label>
          <input type='text' className='form-control' id='nome' name='nome' onChange={handleInputChange} value={formData.nome}/>
        </div>

        <div className='mb-3 mt-3'>
          <label htmlFor='string' className='form-label'>
            String Connection
          </label>
          <input type='text' className='form-control' id='string' name='string' onChange={handleInputChange} value={formData.string}/>
        </div>

        <div className='mb-3 mt-3'>
          <label htmlFor='usuario' className='form-label'>
            User Database
          </label>
          <input type='text' className='form-control' id='usuario' name='usuario' onChange={handleInputChange} value={formData.usuario}/>
        </div>

        <div className='mb-3 mt-3'>
          <label htmlFor='senha' className='form-label'>
            Password
          </label>
          <input type='text' className='form-control' id='senha' name='senha' onChange={handleInputChange} value={formData.senha}/>
        </div>

        <button type='submit' className='btn btn-primary'>
          Submit
        </button>
      </form>


      <table className='table table-striped table-bordered table-hover'>
        <thead>
          <tr>
            <th>Connection Name</th>
            <th>String connection</th>
            <th>User Database</th>
            <th>Password</th>
            
          </tr>
        </thead>
        <tbody>
          {connections.map((connection) =>(
            <tr key={connection.id}>
              <td>{connection.nome}</td>
              <td>{connection.string}</td>
              <td>{connection.usuario}</td>
              <td>{connection.senha}</td>
            </tr>
          ))}
        </tbody>

      </table>



    </div>


    </div>
  )

}

export default App;
